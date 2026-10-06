"""Local-only credential preactivation guard; inert on import, writes no files.
Root integrates one hash-pinned validate_configs() call into the NEW Trial24
shared bootstrap context, after collecting four config dicts and before m.plan.
All plan/preflight/lifecycle callers then share the guard before any SSH/runtime.
No TLS disable/fallback; private key bytes are never returned or printed.
"""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import argparse, hashlib, ipaddress, json, pathlib, ssl, subprocess, time

NODES = ("node-a", "node-b", "node-c", "node-d")
CLUSTER = "gomap-4250-rf4trial24mixedchangingc1"

def _file(name, cap, private=False):
    p = pathlib.Path(name)
    if not p.is_absolute() or p.is_symlink() or not p.is_file():
        raise ValueError("absolute nonsymlink regular credential file required")
    st = p.stat()
    if not 0 < st.st_size <= cap or (private and st.st_mode & 0o077):
        raise ValueError("credential bounds/private permissions")
    return p, (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_mode)

def validate_configs(configs, *, cluster_id=CLUSTER, minimum_remaining_seconds):
    if not isinstance(minimum_remaining_seconds, int) or isinstance(minimum_remaining_seconds, bool) or not 86400 <= minimum_remaining_seconds <= 7*86400:
        raise ValueError("explicit campaign horizon of at least24h required")
    configs = list(configs)
    if len(configs) != 4 or {c["NodeID"] for c in configs} != set(NODES):
        raise ValueError("exact four-node credential inventory required")
    if cluster_id != CLUSTER or any(c["ClusterID"] != cluster_id for c in configs):
        raise ValueError("actual Trial24 ClusterID required")
    public = []
    trust_digest = None
    for c in configs:
        node = c["NodeID"]
        paths = c["Credentials"]
        ca, ca_stat = _file(paths["TrustRootsFile"], 128*1024)
        cert, cert_stat = _file(paths["CertificateFile"], 64*1024)
        key, key_stat = _file(paths["PrivateKeyFile"], 16*1024, True)
        ca_raw, cert_raw = ca.read_bytes(), cert.read_bytes()
        if ca_raw.count(b"-----BEGIN CERTIFICATE-----") != 1 or cert_raw.count(b"-----BEGIN CERTIFICATE-----") != 1:
            raise ValueError("fresh fixture requires one coherent CA and one leaf")
        ca_sha = hashlib.sha256(ca_raw).hexdigest()
        trust_digest = ca_sha if trust_digest is None else trust_digest
        if trust_digest != ca_sha:
            raise ValueError("four nodes must use identical fresh CA bytes")
        root_meta = ssl._ssl._test_decode_cert(str(ca))
        leaf_meta = ssl._ssl._test_decode_cert(str(cert))
        now = time.time()
        for meta in (root_meta, leaf_meta):
            if now < ssl.cert_time_to_seconds(meta["notBefore"]) or now + minimum_remaining_seconds >= ssl.cert_time_to_seconds(meta["notAfter"]):
                raise ValueError("certificate validity/campaign horizon refusal")
        uri = "spiffe://treedb/cluster/" + cluster_id + "/node/" + node
        sans = leaf_meta.get("subjectAltName", ())
        if [v for k,v in sans if k == "URI"] != [uri]:
            raise ValueError("exact cluster/node URI refusal")
        addresses = [c["ListenAddress"], *c["RaftListen"].values()]
        for members in c.get("VectorInitialization", {}).get("ShardAddresses", {}).values():
            if node in members:
                addresses.append(members[node])
        required_ips = {ipaddress.ip_address(a.rsplit(":",1)[0]).compressed for a in addresses}
        advertised = {ipaddress.ip_address(v).compressed for k,v in sans if k == "IP Address"}
        if not required_ips <= advertised:
            raise ValueError("local advertised endpoint IP SAN refusal")
        # load_cert_chain uses native TLS parsing/pair verification; no key output.
        pair = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        pair.load_cert_chain(str(cert), str(key), password=lambda: "")
        for purpose in ("sslclient", "sslserver"):
            r = subprocess.run(["openssl","verify","-no-CApath","-no-CAstore","-CAfile",str(ca),"-purpose",purpose,str(cert)],
                               capture_output=True, timeout=15)
            if r.returncode:
                raise ValueError("CA/signature/validity/EKU verification refusal: " + node + "/" + purpose)
        for path, cap, private, expected in ((ca,128*1024,False,ca_stat),(cert,64*1024,False,cert_stat),(key,16*1024,True,key_stat)):
            if _file(path,cap,private)[1] != expected:
                raise ValueError("credential changed during admission")
        if ca.read_bytes() != ca_raw or cert.read_bytes() != cert_raw:
            raise ValueError("public credential bytes changed during admission")
        public.append({"node":node,"uri":uri,"trust_path":str(ca),"trust_sha256":ca_sha,
                       "certificate_path":str(cert),"certificate_sha256":hashlib.sha256(cert_raw).hexdigest(),
                       "not_before":leaf_meta["notBefore"],"not_after":leaf_meta["notAfter"],
                       "ca_not_before":root_meta["notBefore"],"ca_not_after":root_meta["notAfter"],
                       "required_ip_sans":sorted(required_ips),"pair_checked":True,"both_eku_chains_checked":True})
    finished = time.time()
    if any(finished + minimum_remaining_seconds >= min(ssl.cert_time_to_seconds(v["not_after"]), ssl.cert_time_to_seconds(v["ca_not_after"])) for v in public):
        raise ValueError("certificate horizon elapsed during admission")
    return {"state":"LOCAL_CREDENTIAL_PREACTIVATION_CHECKED_NOT_RUNTIME",
            "cluster_id":cluster_id,"minimum_remaining_seconds":minimum_remaining_seconds,
            "checked_unix":time.time(),"public_credentials":public}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config-dir", required=True)
    p.add_argument("--minimum-remaining-seconds", required=True, type=int)
    a = p.parse_args()
    root = pathlib.Path(a.config_dir)
    if not root.is_absolute():
        raise ValueError("absolute config directory required")
    configs = []
    for node in NODES:
        source, _ = _file(root/(node+".json"), 1024*1024)
        configs.append(json.loads(source.read_bytes()))
    print(json.dumps(validate_configs(configs, minimum_remaining_seconds=a.minimum_remaining_seconds), indent=2))

if __name__ == "__main__":
    main()
