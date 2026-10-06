"""Actual producer-schema compatibility controls; all receipts/tool outputs are synthetic."""
assert len(checks)==369
build_raw=Path(d['build']['path']).read_bytes()
images_raw=Path(d['images']['path']).read_bytes()
acceptance_raw=Path(d['source_prereview']['path']).read_bytes()
assert bp.product_tuple(build_raw,images_raw,acceptance_raw)==normalized
assert bp.frozen_product(tuple_paths)==normalized
assert all(set(v)=={'path','sha256','bytes','metadata'} for v in objects['build']['ELFs'].values())
assert all(set(v)=={'sha256','bytes'} for row in objects['images']['images'].values() for v in row['ELFs'].values())
assert all(objects['build']['ELFs']!=row['ELFs'] for row in objects['images']['images'].values())
checks.append('elf_schema_actual_emission_projection_and_central_tuple_positive')
# The preceding retained controls reached actual context/plan, preflight/lifecycle,
# freezer provenance, growth/post generation and finalization with these schemas.
assert sha(normalized_raw)!=d['build']['sha256']
checks.append('elf_schema_actual_rich_producer_chain_all_existing_closure_joins_positive')
def semantic_tuple(build,images):
 br=json.dumps(build).encode();images=copy.deepcopy(images);images['build_proof_sha256']=sha(br);ir=json.dumps(images).encode()
 previous=copy.deepcopy(bp.PRODUCT_SHA)
 try:
  bp.PRODUCT_SHA.update(build=sha(br),images=sha(ir))
  return bp.product_tuple(br,ir,acceptance_raw)
 finally:bp.PRODUCT_SHA.clear();bp.PRODUCT_SHA.update(previous)
for host in ('111','185'):
 for name in ('treedb-fixed-peer','treedb-query-under-write'):
  for key,value in [('sha256','8'*64),('sha256','0'*64),('sha256','invalid'),('bytes',999),('bytes',0),('bytes',-1),('bytes',True),('bytes',128.0)]:
   value_images=copy.deepcopy(objects['images']);value_images['images'][host]['ELFs'][name][key]=value
   bad('elf_schema_central_'+host+'_'+name+'_'+key+'_'+repr(value),lambda value_images=value_images:semantic_tuple(objects['build'],value_images))
  for kind in ('missing','renamed','extra'):
   value_images=copy.deepcopy(objects['images']);rows=value_images['images'][host]['ELFs']
   if kind=='missing':rows.pop(name)
   elif kind=='renamed':rows['other-executable']=rows.pop(name)
   else:rows['extra-executable']=copy.deepcopy(rows[name])
   bad('elf_schema_central_'+host+'_'+name+'_'+kind+'_name',lambda value_images=value_images:semantic_tuple(objects['build'],value_images))
for kind in ('missing_host','extra_host'):
 value_images=copy.deepcopy(objects['images'])
 if kind=='missing_host':value_images['images'].pop('111')
 else:value_images['images']['186']=copy.deepcopy(value_images['images']['185'])
 bad('elf_schema_central_'+kind,lambda value_images=value_images:semantic_tuple(objects['build'],value_images))
for name in ('treedb-fixed-peer','treedb-query-under-write'):
 for key,value in [('sha256','0'*64),('sha256','invalid'),('bytes',0),('bytes',-1),('bytes',True),('bytes',128.0)]:
  value_build=copy.deepcopy(objects['build']);value_build['ELFs'][name][key]=value
  value_images=copy.deepcopy(objects['images'])
  for row in value_images['images'].values():row['ELFs'][name][key]=value
  bad('elf_schema_central_matching_invalid_build_image_'+name+'_'+key+'_'+repr(value),lambda value_build=value_build,value_images=value_images:semantic_tuple(value_build,value_images))
for kind in ('missing','extra'):
 value_build=copy.deepcopy(objects['build'])
 if kind=='missing':value_build['ELFs'].pop('treedb-fixed-peer')
 else:value_build['ELFs']['extra-executable']=copy.deepcopy(value_build['ELFs']['treedb-fixed-peer'])
 bad('elf_schema_central_build_'+kind+'_name',lambda value_build=value_build:semantic_tuple(value_build,objects['images']))
# Metadata is preserved; immutable raw hashes remain authority before projection.
value_build=copy.deepcopy(objects['build']);value_build['ELFs']['treedb-fixed-peer']['metadata']['file']['stdout']+=' changed synthetic metadata'
br=json.dumps(value_build).encode()
bad('elf_schema_metadata_change_without_raw_repin_rejected',lambda:bp.product_tuple(br,images_raw,acceptance_raw))
assert semantic_tuple(value_build,objects['images'])==normalized
checks.append('elf_schema_explicit_synthetic_metadata_repin_same_normalized_tuple_positive')
row=copy.deepcopy(d);row['build']=fixture('elf-schema-synthetic-metadata-repin-build',value_build)
value_images=copy.deepcopy(objects['images']);value_images['build_proof_sha256']=row['build']['sha256'];row['images']=fixture('elf-schema-synthetic-metadata-repin-images',value_images)
x.declaration(row,m);checks.append('elf_schema_actual_declaration_rich_metadata_repin_positive')
for name in ('treedb-fixed-peer','treedb-query-under-write'):
 for kind in ('path','file','go','ldd'):
  elves=copy.deepcopy(synthetic_build_elves)
  if kind=='path':elves[name]['path']+='.other'
  elif kind=='file':elves[name]['metadata']['file']['stdout']='invalid architecture'
  elif kind=='go':elves[name]['metadata']['go-version-m']['stdout']='invalid Go metadata'
  else:elves[name]['metadata']['ldd']={'exit':2,'stdout':'','stderr':'failed'}
  bad('elf_schema_actual_packaging_guard_'+name+'_'+kind+'_rejects',lambda elves=elves:actual_image_projection(elves))
(fixtures/'elf-schema-integration.json').write_text(json.dumps({'state':'SYNTHETIC_ACTUAL_PRODUCER_SCHEMA_CLOSURE_NOT_ACCEPTANCE','build':d['build'],'images':d['images'],'original_raw_hashes_preserved':True,'build_schema':['path','sha256','bytes','metadata'],'image_schema':['sha256','bytes'],'adapter_source_sha256':{p.name:sha(p.read_bytes()) for p in (R/'root-adapters').glob('*.py')},'actual_emission_projection_executed':True,'external_tool_outputs':'controlled synthetic file/ldd/go-version-m only','normalized_raw_sha256':sha(normalized_raw),'actual_affected_chain':['declaration/instantiation','bootstrap context/plan','preflight/preparation/lifecycle joins','freezer bootstrap_provenance','growth_product','post_product/finalization'],'added_controls':checks[369:],'runtime_started':False,'Go_started':False,'network_calls':0,'limitations':['Receipt assignment and validated projection execute with synthetic ELF-shaped bytes and controlled tool metadata; no actual Go build/image/package/runtime acceptance.','Existing consumer controls retain mocked dataset/TLS dependencies and no external activation.']},indent=2)+'\n')
