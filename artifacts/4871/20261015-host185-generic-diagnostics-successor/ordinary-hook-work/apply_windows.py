from pathlib import Path
w=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-repair')
p=w/'TreeDB/db/vacuum_original_completion_test.go';s=p.read_text().replace('\n\t"syscall"','')
# retained offline LOCK shutdown only after full sticky/refund/lock oracle.
a=s.index('func TestOfflineVacuumOriginalSourceCloseRefusalRetainsLock');b=s.index('\nfunc ',a+1);chunk=s[a:b];chunk=chunk.rstrip()[:-1]+''' // Modeled process shutdown follows all fatal custody and no-replay checks.
 if err = g.startupLock.Close(); err != nil { t.Fatal(err) }
}
''';s=s[:a]+chunk+s[b:]
a=s.index('func TestPrimaryJointNamespaceRebindKeepsOriginalParents');b=s.index('\nfunc ',a+1);chunk=s[a:b]
chunk=chunk.replace('\t\t\tif fault {\n\t\t\t\tif err = dataParent.Close()', '\t\t\tvar originalParentError error\n\t\t\tif fault {\n\t\t\t\tif err = dataParent.Close()',1)
chunk=chunk.replace('\t\t\tfirst := g.rebindPrimaryJointNamespaceV6(dir)', '''            if fault {
                _, originalParentError = rootpublication.StableNamespaceParentGeneration(dataParent)
                if originalParentError == nil { t.Fatal("closed original parent remained usable") }
                if cause := errors.Unwrap(originalParentError); cause != nil { originalParentError = cause }
            }
            originalLock := d.lock
            first := g.rebindPrimaryJointNamespaceV6(dir)''')
chunk=chunk.replace('!errors.Is(first, syscall.EBADF) && !errors.Is(first, os.ErrClosed)', '!errors.Is(first, originalParentError)')
chunk=chunk.replace('if err = d.Close(); !errors.Is(err, os.ErrClosed)', 'if err = d.Close(); !errors.Is(err, first)')
needle='\t\t\t\t\tt.Fatalf("terminal custody lost: %v", err)\n\t\t\t\t}'
chunk=chunk.replace(needle,needle+'''\n // Explicit process shutdown after original error/custody/no-refund assertions.
 // Do not replay the failed parent primitive or complete its metadata receipt.
 _ = g.pager.Close(); _ = g.primary.Pager().Close()
 if err = primaryParent.Close(); err != nil { t.Fatal(err) }
 if err = originalLock.Close(); err != nil { t.Fatal(err) }''')
s=s[:a]+chunk+s[b:]
a=s.index('func TestOfflineSourceLateNamespaceRefusalRetainsOriginalLock');chunk=s[a:]
needle='\t\t\tsource := d.idx.Load()';chunk=chunk.replace(needle,'''            if primaryOfflineScopePlatformRefusal(t, d) {
                if err = d.Close(); err != nil { t.Fatal(err) }
                if err = lock.Close(); err != nil { t.Fatal(err) }
                next, e := lockfile.Acquire(filepath.Join(dir,"LOCK")); if e != nil { t.Fatal(e) }; if e = next.Close(); e != nil { t.Fatal(e) }; return
            }
'''+needle,1)
# Late failed parent LOCK explicitly remains retained until modeled shutdown.
needle='\t\t\t\tt.Fatalf("late namespace refusal released LOCK: %v", lockErr)\n\t\t\t}'
chunk=chunk.replace(needle,needle+'''\n // All sticky failure and retained LOCK assertions precede process shutdown.
 if err = lock.Close(); err != nil { t.Fatal(err) }''')
s=s[:a]+chunk;p.write_text(s)
p=w/'TreeDB/db/stable_pager_owned_operations_test.go';s=p.read_text();a=s.index('func TestOwnedPagerOperationPostConsumptionVLogFailureRetainsGovernor');b=s.index('\nfunc ',a+1);chunk=s[a:b];chunk=chunk.replace('\tphysicalPager := snapshot.primaryRoot.arena.Pager()', '\tphysicalPager := snapshot.primaryRoot.arena.Pager()\n originalIndex, originalLock := d.idx.Load(), d.lock')
chunk=chunk.replace('\t\t_ = physicalPager.Close()', '''        _ = physicalPager.Close()
        _ = originalIndex.pager.Close()
        if originalIndex.stableNamespaceParent != nil { _ = originalIndex.stableNamespaceParent.Close() }
        if originalIndex.primaryOwner.namespaceParent != nil { _ = originalIndex.primaryOwner.namespaceParent.Close() }
        if originalLock != nil { _ = originalLock.Close() }''');s=s[:a]+chunk+s[b:];p.write_text(s)
p=w/'TreeDB/collections/api_test.go';s=p.read_text().replace('"github.com/snissn/gomap/TreeDB/internal/retainedalloc"','"github.com/snissn/gomap/TreeDB/internal/retainedalloc"\n "github.com/snissn/gomap/TreeDB/internal/rootpublication"');a=s.index('func TestCollectionValueLogRewriteOffline_RoundTripWithCompressedSecondaryIndexes');b=s.index('\nfunc ',a+1);chunk=s[a:b]
needle='\trewriteStats, err := treedb.ValueLogRewriteOffline(opts)';chunk=chunk.replace(needle,'''    images := func() map[string][]byte {
        result := make(map[string][]byte)
        if e := filepath.WalkDir(opts.Dir, func(path string, entry os.DirEntry, e error) error {
            if e != nil { return e }; if entry.IsDir() || entry.Name()=="LOCK" { return nil }; image,e:=os.ReadFile(path); if e != nil { return e }; result[path]=image;return nil
        }); e != nil { t.Fatal(e) }; return result
    }
    var before map[string][]byte
    if !rootpublication.StableRelativeNamespaceSupported() { before=images() }
'''+needle)
old='''\tif err != nil {
\t\tt.Fatalf("ValueLogRewriteOffline: %v", err)
\t}
\tif rewriteStats.RecordsCopied == 0 {
\t\tt.Fatalf("expected offline rewrite to copy collection leaf records, stats=%+v", rewriteStats)
\t}''';new='''    if before != nil {
        if !errors.Is(err,rootpublication.ErrNamespacePersistenceUnsupported) { t.Fatalf("unsupported offline rewrite: %v",err) }
        if !reflect.DeepEqual(before, images()) { t.Fatal("unsupported offline rewrite changed persistent inputs") }
    } else {
        if err != nil { t.Fatalf("ValueLogRewriteOffline: %v",err) }
        if rewriteStats.RecordsCopied == 0 { t.Fatalf("expected offline rewrite to copy collection leaf records, stats=%+v",rewriteStats) }
    }''';assert old in chunk;chunk=chunk.replace(old,new);s=s[:a]+chunk+s[b:];p.write_text(s)
