from __future__ import annotations

import argparse
import json
from pathlib import Path

import torch
import torch.nn.functional as F
from huggingface_hub.utils import logging as hf_hub_logging
from transformers import AutoModel, AutoTokenizer
from transformers.utils import logging as transformers_logging


MODEL_NAME = "google/bert_uncased_L-2_H-128_A-2"
hf_hub_logging.set_verbosity_error()
transformers_logging.disable_progress_bar()
transformers_logging.set_verbosity_error()

DOCUMENTS = [
    "A tiny BERT model can create compact embeddings for short text snippets.",
    "Sentence embeddings map pieces of text into vectors that can be compared.",
    "Cosine similarity measures how closely two embedding vectors point together.",
    "The cat stretched on the sunny windowsill after chasing a toy mouse.",
    "A puppy learned to fetch a tennis ball in the park.",
    "Fresh basil, tomatoes, and mozzarella make a quick summer salad.",
    "The chef simmered onions and garlic before adding pasta sauce.",
    "Astronomers observed a bright comet passing near Jupiter.",
    "A satellite captured new images of storms swirling over the ocean.",
    "The basketball team practiced fast breaks before the playoff game.",
    "A tennis player served three aces during the final set.",
    "Stock prices rose after the company reported stronger quarterly earnings.",
    "Investors watched bond yields and inflation data before the market opened.",
    "The hiking trail climbed through pine trees to a cold mountain lake.",
    "Campers packed a tent, rain jacket, and water filter for the weekend.",
    "The software release fixed a memory leak and improved startup time.",
    "Engineers wrote unit tests before refactoring the database layer.",
    "A doctor reviewed the patient's blood pressure and lab results.",
    "The clinic scheduled follow-up visits for vaccination and screening.",
    "Rain clouds moved inland as the weather forecast warned of thunderstorms.",
]


def mean_pool(last_hidden_state: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
    mask = attention_mask.unsqueeze(-1).type_as(last_hidden_state)
    summed = (last_hidden_state * mask).sum(dim=1)
    counts = mask.sum(dim=1).clamp(min=1e-9)
    return summed / counts


def embed_documents(documents: list[str]) -> torch.Tensor:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME, dtype="auto")
    model.eval()

    encoded = tokenizer(
        documents,
        padding=True,
        truncation=True,
        max_length=128,
        return_tensors="pt",
    )

    with torch.inference_mode():
        output = model(**encoded)
        embeddings = mean_pool(output.last_hidden_state, encoded["attention_mask"])

    return F.normalize(embeddings, p=2, dim=1)


def print_similarity_matrix(similarities: torch.Tensor) -> None:
    labels = [f"D{i + 1:02d}" for i in range(similarities.shape[0])]

    print("Pairwise cosine similarity matrix")
    print("     " + " ".join(f"{label:>6}" for label in labels))
    for label, row in zip(labels, similarities):
        values = " ".join(f"{score.item():6.3f}" for score in row)
        print(f"{label:>4} {values}")


def print_top_pairs(similarities: torch.Tensor, documents: list[str], limit: int = 10) -> None:
    pairs = []
    for left in range(similarities.shape[0]):
        for right in range(left + 1, similarities.shape[1]):
            pairs.append((similarities[left, right].item(), left, right))

    pairs.sort(reverse=True)

    print(f"\nTop {limit} most similar document pairs")
    for score, left, right in pairs[:limit]:
        print(f"{score:6.3f}  D{left + 1:02d} <-> D{right + 1:02d}")
        print(f"        D{left + 1:02d}: {documents[left]}")
        print(f"        D{right + 1:02d}: {documents[right]}")


def write_jsonl(path: Path, embeddings: torch.Tensor, documents: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for i, (document, embedding) in enumerate(zip(documents, embeddings), start=1):
            record = {
                "id": f"D{i:02d}",
                "text": document,
                "model": MODEL_NAME,
                "pooling": "mean",
                "normalized": True,
                "embedding": [float(value) for value in embedding.tolist()],
            }
            f.write(json.dumps(record, separators=(",", ":")))
            f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        help="write TreeDB benchmark fixture records with text and embeddings",
    )
    args = parser.parse_args()

    embeddings = embed_documents(DOCUMENTS)
    similarities = embeddings @ embeddings.T

    print(f"Generated {len(DOCUMENTS)} embeddings with dimension {embeddings.shape[1]}.")
    if args.output_jsonl is not None:
        write_jsonl(args.output_jsonl, embeddings, DOCUMENTS)
        print(f"Wrote JSONL embeddings to {args.output_jsonl}.")
    print_similarity_matrix(similarities)
    print_top_pairs(similarities, DOCUMENTS)


if __name__ == "__main__":
    main()
