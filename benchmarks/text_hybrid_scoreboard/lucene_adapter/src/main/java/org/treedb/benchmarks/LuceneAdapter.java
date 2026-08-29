package org.treedb.benchmarks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public final class LuceneAdapter {
  private static final String RESULT_SCHEMA = "treedb_lexical_result/v1";
  private static final String LUCENE_VERSION = "9.12.1";
  private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private record CorpusDocument(String id, String title, String body, String tenant) {}
  private record SearchRun(List<String> ids, String queryType) {}

  public static void main(String[] argv) throws Exception {
    Map<String, String> args = parseArgs(argv);
    Path manifestPath = requiredPath(args, "--manifest");
    Path corpusPath = requiredPath(args, "--corpus");
    Path outPath = requiredPath(args, "--out");
    Path indexPath = requiredPath(args, "--index");
    int repetition = Integer.parseInt(required(args, "--repetition"));
    byte[] manifestRaw = Files.readAllBytes(manifestPath);
    byte[] corpusRaw = Files.readAllBytes(corpusPath);
    JsonNode manifest = JSON.readTree(manifestRaw);
    List<CorpusDocument> documents = parseCorpus(corpusRaw);
    int documentCount = manifest.at("/corpus/document_count").asInt();
    if (documents.size() != documentCount) throw new IllegalArgumentException("corpus document count drift");
    deleteTree(indexPath);
    Files.createDirectories(indexPath);

    long cpuBefore = processCpuNanos();
    long buildStart = System.nanoTime();
    try (FSDirectory directory = FSDirectory.open(indexPath)) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      config.setSimilarity(new BM25Similarity(1.2f, 0.75f));
      config.setUseCompoundFile(false);
      try (IndexWriter writer = new IndexWriter(directory, config)) {
        for (CorpusDocument source : documents) {
          Document doc = new Document();
          doc.add(new StringField("id", source.id(), Field.Store.YES));
          doc.add(new SortedDocValuesField("id_sort", new BytesRef(source.id())));
          doc.add(new TextField("title", source.title(), Field.Store.NO));
          doc.add(new TextField("body", source.body(), Field.Store.NO));
          doc.add(new StringField("tenant", source.tenant(), Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.commit();
      }
    }
    long buildElapsed = System.nanoTime() - buildStart;
    long buildCpu = processCpuNanos() - cpuBefore;
    long durableBytes = directoryBytes(indexPath);

    int topK = manifest.at("/execution/top_k").asInt();
    int warmup = manifest.at("/execution/warmup_queries_per_case").asInt();
    int measured = manifest.at("/execution/measured_queries_per_case").asInt();
    List<Map<String, Object>> cases = new ArrayList<>();
    try (FSDirectory directory = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      searcher.setSimilarity(new BM25Similarity(1.2f, 0.75f));
      for (JsonNode query : manifest.path("queries")) {
        for (int i = 0; i < warmup; i++) execute(searcher, query, topK);
        List<Long> samples = new ArrayList<>();
        SearchRun last = null;
        for (int i = 0; i < measured; i++) {
          long start = System.nanoTime();
          last = execute(searcher, query, topK);
          samples.add(System.nanoTime() - start);
        }
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", query.path("id").asText()); item.put("status", "ok"); item.put("equivalent", true);
        item.put("samples_nanos", samples); item.put("result_ids", last.ids()); item.put("result_digest", digestIDs(last.ids()));
        item.put("route", Map.of("intended", true, "name", "lucene_inverted_index_bm25", "fallback", false, "proof", Map.of("query_class", last.queryType(), "reader_documents", reader.numDocs())));
        item.put("timed_out", false); cases.add(item);
      }
    }

    boolean reopenVerified = true;
    try (FSDirectory directory = FSDirectory.open(indexPath); DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader); searcher.setSimilarity(new BM25Similarity(1.2f, 0.75f));
      int i = 0;
      for (JsonNode query : manifest.path("queries")) {
        List<String> ids = execute(searcher, query, topK).ids();
        Map<String, Object> item = cases.get(i++);
        item.put("reopen_result_ids", ids); item.put("reopen_result_digest", digestIDs(ids));
        reopenVerified &= item.get("result_digest").equals(item.get("reopen_result_digest"));
      }
    }

    String execArgs = String.join(" ", argv);
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("schema_version", RESULT_SCHEMA); payload.put("status", "ok");
    payload.put("engine", Map.of("id", "lucene", "family", "lucene_family", "name", "Apache Lucene", "version", LUCENE_VERSION));
    payload.put("repetition", repetition); payload.put("manifest_sha256", sha256(canonicalManifest(manifestRaw)));
    payload.put("corpus", Map.of("document_count", documents.size(), "sha256", sha256(corpusRaw)));
    payload.put("command", List.of("mvn", "-q", "compile", "exec:java", "-Dexec.args=" + execArgs));
    payload.put("versions", Map.of("lucene", Version.LATEST.toString(), "java", System.getProperty("java.version"), "vm", System.getProperty("java.vm.name"), "platform", System.getProperty("os.name") + "/" + System.getProperty("os.arch")));
    payload.put("config", Map.of("working_directory", System.getProperty("user.dir"), "analyzer", "StandardAnalyzer", "similarity", "BM25(k1=1.2,b=0.75)", "weights", Map.of("title", 3.0, "body", 1.0), "top_k", topK, "tie_break", "score,id", "compound_file", false, "stored_fields", List.of("id")));
    payload.put("build", mapOfNullable("elapsed_nanos", buildElapsed, "docs_per_second", documents.size() * 1e9 / buildElapsed, "cpu_nanos", buildCpu, "peak_rss_bytes", null, "checkpointed", true));
    payload.put("storage", Map.of("durable_bytes", durableBytes, "wal_bytes", 0, "transient_bytes", 0));
    payload.put("reopen", Map.of("performed", true, "verified", reopenVerified, "result_digest", digestCaseResults(cases)));
    payload.put("cases", cases);
    Files.createDirectories(outPath.getParent());
    JSON.writerWithDefaultPrettyPrinter().writeValue(outPath.toFile(), payload);
  }

  private static SearchRun execute(IndexSearcher searcher, JsonNode spec, int topK) throws IOException {
    String semantic = spec.path("semantic").asText();
    List<String> terms = new ArrayList<>(); spec.path("terms").forEach(term -> terms.add(term.asText()));
    Query query;
    if (semantic.equals("term") || semantic.equals("term_scalar")) query = weightedTerm(terms.get(0));
    else if (semantic.equals("and")) query = new BooleanQuery.Builder().add(weightedTerm(terms.get(0)), BooleanClause.Occur.MUST).add(weightedTerm(terms.get(1)), BooleanClause.Occur.MUST).build();
    else if (semantic.equals("or")) query = new BooleanQuery.Builder().add(weightedTerm(terms.get(0)), BooleanClause.Occur.SHOULD).add(weightedTerm(terms.get(1)), BooleanClause.Occur.SHOULD).setMinimumNumberShouldMatch(1).build();
    else if (semantic.equals("phrase")) {
      Query title = new BoostQuery(new PhraseQuery(0, "title", terms.toArray(String[]::new)), 3f);
      Query body = new PhraseQuery(0, "body", terms.toArray(String[]::new));
      query = new BooleanQuery.Builder().add(title, BooleanClause.Occur.SHOULD).add(body, BooleanClause.Occur.SHOULD).setMinimumNumberShouldMatch(1).build();
    } else throw new IllegalArgumentException("unsupported semantic " + semantic);
    if (spec.has("filter")) query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST).add(new TermQuery(new Term(spec.at("/filter/field").asText(), spec.at("/filter/equals").asText())), BooleanClause.Occur.FILTER).build();
    TopFieldDocs hits = searcher.search(query, topK, new Sort(SortField.FIELD_SCORE, new SortField("id_sort", SortField.Type.STRING)));
    List<String> ids = new ArrayList<>(); for (ScoreDoc hit : hits.scoreDocs) ids.add(searcher.storedFields().document(hit.doc).get("id"));
    return new SearchRun(ids, query.getClass().getSimpleName());
  }

  private static Query weightedTerm(String term) {
    Query title = new BoostQuery(new TermQuery(new Term("title", term)), 3f);
    Query body = new TermQuery(new Term("body", term));
    return new BooleanQuery.Builder().add(title, BooleanClause.Occur.SHOULD).add(body, BooleanClause.Occur.SHOULD).setMinimumNumberShouldMatch(1).build();
  }

  private static List<CorpusDocument> parseCorpus(byte[] raw) {
    List<CorpusDocument> documents = new ArrayList<>();
    for (String line : new String(raw, StandardCharsets.UTF_8).split("\\n")) {
      if (line.isEmpty()) continue; String[] values = line.split("\\t", -1);
      if (values.length != 4) throw new IllegalArgumentException("invalid corpus TSV row");
      documents.add(new CorpusDocument(values[0], values[1], values[2], values[3]));
    }
    return documents;
  }

  private static byte[] canonicalManifest(byte[] raw) throws IOException { Object value = JSON.readValue(raw, new TypeReference<Object>() {}); byte[] encoded = JSON.writeValueAsBytes(value); byte[] withNewline = java.util.Arrays.copyOf(encoded, encoded.length + 1); withNewline[encoded.length] = '\n'; return withNewline; }
  private static String digestIDs(List<String> ids) throws Exception { return sha256((String.join("\n", ids) + (ids.isEmpty() ? "" : "\n")).getBytes(StandardCharsets.UTF_8)); }
  private static String digestCaseResults(List<Map<String, Object>> cases) throws Exception { List<String> values = new ArrayList<>(); for (Map<String, Object> item : cases) values.add((String)item.get("reopen_result_digest")); return digestIDs(values); }
  private static String sha256(byte[] value) throws Exception { return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); }
  private static long directoryBytes(Path root) throws IOException { try (var paths = Files.walk(root)) { return paths.filter(Files::isRegularFile).mapToLong(path -> { try { return Files.size(path); } catch (IOException e) { throw new RuntimeException(e); } }).sum(); } }
  private static long processCpuNanos() { Optional<Duration> duration = ProcessHandle.current().info().totalCpuDuration(); return duration.map(Duration::toNanos).orElse(0L); }
  private static void deleteTree(Path path) throws IOException { if (!Files.exists(path)) return; try (var paths = Files.walk(path)) { paths.sorted(java.util.Comparator.reverseOrder()).forEach(item -> { try { Files.delete(item); } catch (IOException e) { throw new RuntimeException(e); } }); } }
  private static Map<String, Object> mapOfNullable(Object... values) { Map<String, Object> result = new LinkedHashMap<>(); for (int i = 0; i < values.length; i += 2) result.put((String)values[i], values[i + 1]); return result; }
  private static Map<String, String> parseArgs(String[] args) { if (args.length % 2 != 0) throw new IllegalArgumentException("arguments must be name/value pairs"); Map<String, String> result = new LinkedHashMap<>(); for (int i = 0; i < args.length; i += 2) result.put(args[i], args[i + 1]); return result; }
  private static String required(Map<String, String> args, String name) { String value = args.get(name); if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required"); return value; }
  private static Path requiredPath(Map<String, String> args, String name) { return Path.of(required(args, name)); }
}
