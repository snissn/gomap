package db

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"testing"
)

// These certificates describe an error's complete Go method surface, not its
// physical payload. An opaque Is callback can call those methods but cannot
// directly access private fields. A source change fails closed against these
// bindings; a matching hash detects no drift but does not establish semantic
// correctness. Callback bodies and every retained error child remain subject to
// the ordinary source analysis and the runtime review described in verification.md.
var publicationGuardCustodyCertificates = map[string]string{
	"errors.joinError": "49915178189d923b36d5d74dbcd3c6122074c406a25e114f5db931c413dca2ab",
	"fmt.wrapError":    "007335e22e8290a1a5ba1a46cdd63424ec5cfe897b6e225fd5f50b2bcdaefd7f",
	"fmt.wrapErrors":   "a93ac802b168486e18a7bb3c9bdc93cdc604e99d80e8770dedde943cb4cb3500",
	publicationGuardPackagePath + ".OpenRecoveryCleanupError":                        "75396e4ed360373e40fc1df13a9739ecbc8c796cf43f13dff2dc131be0210454",
	publicationGuardPackagePath + ".originalDBCleanupFailure":                        "571d19897cd55a2a3d45e486055c123c9c502ff5d58a61b04a0c29b96cbadbf3",
	"github.com/snissn/gomap/TreeDB/internal/rootpublication.originalReleaseFailure": "cab30749ed7a96f9e341d31a2414b3af218f260342493943f9007c21a6643656",
	"github.com/snissn/gomap/TreeDB/pager.ReadOnlyCleanupError":                      "0811e41f33d48d2f1fe1399067766aef0ece8e0201d2b7c9cce6760f62334fc2",
	"github.com/snissn/gomap/TreeDB/internal/valuelog.ManagerCleanupFailure":         "74a125ebab86f8a5c8cfa3120df2c15e1937068a4c6949d89ecf9139475acd3b",
	"github.com/snissn/gomap/TreeDB/internal/valuelog.FileCleanupFailure":            "f7e60cbbb3a67416980489ef4b076ea0bec66ff3e3e7a2de96cc3ccb68267a2f",
}

// This concrete return value exposes only a mutex and error-header binding.
// Its entire method surface and its two constructors are part of the proof.
var publicationGuardCustodyAuxiliaryTypes = map[string]string{
	"github.com/snissn/gomap/TreeDB/internal/rootpublication.OriginalCleanupPriorBinding": "15ce124dcfa5bb9e16a723c95fdbcb0d08f740daeebf884121abfa7b1cac0b2a",
}
var publicationGuardCustodyAuxiliaryFunctions = map[string]string{
	"github.com/snissn/gomap/TreeDB/internal/rootpublication.BindOriginalCleanupPrior":   "5c052a252a490680ce7d03c0f4202517415907b93d984fb5cdcd067a67bb3438",
	"github.com/snissn/gomap/TreeDB/internal/rootpublication.BindOriginalCleanupPriorRW": "c5bf2a8580ef98c0ef42aa53061542b9861072cbbe0dfaf886eead61e7a07974",
}

// Payload fields cannot be read outside exact observers of ownership, original
// error links, or the already bound mutex/error-header capability. These bodies
// also undergo normal callback analysis. Writes preserve original custody.
var publicationGuardCustodyOpaqueFields = map[string][]string{
	publicationGuardPackagePath + ".OpenRecoveryCleanupError":                        {"proof", "generation"},
	publicationGuardPackagePath + ".originalDBCleanupFailure":                        {"payload"},
	"github.com/snissn/gomap/TreeDB/internal/rootpublication.originalReleaseFailure": {"original"},
	"github.com/snissn/gomap/TreeDB/pager.ReadOnlyCleanupError":                      {"pager"},
	"github.com/snissn/gomap/TreeDB/internal/valuelog.ManagerCleanupFailure":         {"manager"},
	"github.com/snissn/gomap/TreeDB/internal/valuelog.FileCleanupFailure":            {"file"},
}
var publicationGuardCustodyReadObservers = map[string]string{
	publicationGuardPackagePath + ".recoveryCleanupOwnsLockV6":                                                "dadf806e18009e52f07c55c3a9d9845503e5e8d051545d99215e20fc64ba0426",
	"github.com/snissn/gomap/TreeDB/internal/valuelog.(*ManagerCleanupFailure).OriginalCleanupPriorAvailable": "6ecaad691e41edd27f7d10de41f4853b6677accd0c9c8e9fd506f689286c506d",
	"github.com/snissn/gomap/TreeDB/internal/valuelog.(*ManagerCleanupFailure).OriginalCleanupPriorBinding":   "17bab6fb55bfa9d9c359e84e609ead6070e562d43d372a91aca4014486afe013",
	"github.com/snissn/gomap/TreeDB/internal/valuelog.(*ManagerCleanupFailure).TryLinkPriorCleanupError":      "ed2bba824e08fbdae65959bfa42c5978743c191307223ca46890fc6c4b3dbbfc",
	"github.com/snissn/gomap/TreeDB/internal/valuelog.(*ManagerCleanupFailure).Is":                            "f016f6173c45b34fae191c776e03d78925886f7ef1e47f00fe661c14ed7819fd",
}

// Include the entire certified method surface in ordinary body analysis, even
// when only a smaller adapter corpus is otherwise loaded for that dependency.
func publicationGuardCustodyMethod(fn *types.Func) bool {
	if fn == nil {
		return false
	}
	signature, ok := fn.Type().(*types.Signature)
	if !ok || signature.Recv() == nil {
		return false
	}
	receiver := types.Unalias(signature.Recv().Type())
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = types.Unalias(pointer.Elem())
	}
	named, ok := receiver.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	_, certified := publicationGuardCustodyCertificates[named.Obj().Pkg().Path()+"."+named.Obj().Name()]
	return certified
}

func publicationGuardCapsuleShape(source *publicationGuardSource, decls map[*types.Func]*ast.FuncDecl, typ types.Type) (string, string, bool) {
	ptr, ok := types.Unalias(typ).(*types.Pointer)
	if !ok {
		return "", "", false
	}
	named, ok := types.Unalias(ptr.Elem()).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return "", "", false
	}
	shape, ok := named.Underlying().(*types.Struct)
	if !ok {
		return "", "", false
	}
	for i := 0; i < shape.NumFields(); i++ {
		if shape.Field(i).Exported() || shape.Field(i).Anonymous() {
			return "", "", false
		}
	}
	key := named.Obj().Pkg().Path() + "." + named.Obj().Name()
	parts := []string{key, types.TypeString(shape, publicationGuardQualifier)}
	for i := 0; i < named.NumMethods(); i++ {
		method := named.Method(i)
		decl := decls[method]
		if decl == nil || decl.Body == nil {
			return key, "", false
		}
		parts = append(parts, publicationGuardFuncID(method)+"\n"+publicationGuardRender(source.fset, decl))
	}
	sort.Strings(parts[2:])
	return key, fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(parts, "\n")))), true
}

func publicationGuardCustodyFiles(source *publicationGuardSource) []*ast.File {
	return append(append([]*ast.File(nil), source.files...), source.custodyFiles...)
}

func publicationGuardCustodyDecls(source *publicationGuardSource) map[*types.Func]*ast.FuncDecl {
	decls := make(map[*types.Func]*ast.FuncDecl)
	for _, file := range publicationGuardCustodyFiles(source) {
		for _, declaration := range file.Decls {
			if declaration, ok := declaration.(*ast.FuncDecl); ok {
				if fn, ok := source.info.Defs[declaration.Name].(*types.Func); ok && declaration.Body != nil {
					decls[fn] = declaration
				}
			}
		}
	}
	return decls
}

func publicationGuardOpaqueFieldReadsClosed(source *publicationGuardSource, decls map[*types.Func]*ast.FuncDecl, fields map[*types.Var]bool, observers map[string]string) bool {
	allowed := make(map[*ast.SelectorExpr]bool)
	for fn, decl := range decls {
		writes := make(map[*ast.SelectorExpr]bool)
		ast.Inspect(decl.Body, func(node ast.Node) bool {
			if assignment, ok := node.(*ast.AssignStmt); ok && assignment.Tok == token.ASSIGN {
				for _, lhs := range assignment.Lhs {
					if selector, ok := publicationGuardUnparen(lhs).(*ast.SelectorExpr); ok {
						writes[selector] = true
					}
				}
			}
			return true
		})
		closed := true
		ast.Inspect(decl.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			selection := source.info.Selections[selector]
			if selection == nil {
				return true
			}
			field, ok := selection.Obj().(*types.Var)
			if !ok || !fields[field] {
				return true
			}
			if writes[selector] {
				allowed[selector] = true
				return true
			}
			want := observers[publicationGuardFuncID(fn)]
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(publicationGuardRender(source.fset, decl))))
			if want == "" || want != digest {
				closed = false
			} else {
				allowed[selector] = true
			}
			return true
		})
		if !closed {
			return false
		}
	}
	// Package initializers do not have a FuncDecl. Include them in the read
	// inventory so a new global alias cannot bypass the private-field proof.
	for _, file := range publicationGuardCustodyFiles(source) {
		closed := true
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || allowed[selector] {
				return true
			}
			if selection := source.info.Selections[selector]; selection != nil {
				if field, ok := selection.Obj().(*types.Var); ok && fields[field] {
					closed = false
				}
			}
			return true
		})
		if !closed {
			return false
		}
	}
	return true
}

func (ta *publicationGuardTaintAnalysis) custodyAuxiliariesClosed() bool {
	found := make(map[string]bool)
	for fn, decl := range ta.funcDecls {
		id := publicationGuardFuncID(fn)
		if want, exists := publicationGuardCustodyAuxiliaryFunctions[id]; exists {
			if want == "" || fmt.Sprintf("%x", sha256.Sum256([]byte(publicationGuardRender(ta.source.fset, decl)))) != want {
				return false
			}
			found[id] = true
		}
		if fn.Pkg() == nil {
			continue
		}
		for key, want := range publicationGuardCustodyAuxiliaryTypes {
			if !strings.HasPrefix(key, fn.Pkg().Path()+".") {
				continue
			}
			object := fn.Pkg().Scope().Lookup(strings.TrimPrefix(key, fn.Pkg().Path()+"."))
			if object == nil {
				return false
			}
			_, got, ok := publicationGuardCapsuleShape(ta.source, ta.funcDecls, types.NewPointer(object.Type()))
			if !ok || want == "" || got != want {
				return false
			}
			found[key] = true
		}
	}
	return len(found) == len(publicationGuardCustodyAuxiliaryTypes)+len(publicationGuardCustodyAuxiliaryFunctions)
}

// Limit the use of this proof to the actual pinned errors.is target callback.
// All receiver callbacks, normal Is/As/Unwrap calls and their bodies keep the
// ordinary source analysis. In particular this is not an error-interface rule.
func (ta *publicationGuardTaintAnalysis) privateCustodyTargetArgument(caller *types.Func, call *ast.CallExpr, arg ast.Expr) bool {
	ta.custodyRefusal = ""
	if caller == nil || caller.Pkg() == nil || caller.Pkg().Path() != "errors" || caller.Name() != "is" || len(call.Args) != 1 || call.Args[0] != arg {
		return false
	}
	selector, ok := publicationGuardUnparen(call.Fun).(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Is" {
		return false
	}
	if !ta.verifySource("errors/wrap.go", "098116636610ae87dd80e49c4dbe6a2b6c37918d7e1af35ded005be621d68a40") || !ta.verifySource("fmt/errors.go", "01fd304868493a3aa1198cc79a56274930f687353e9405211ca063c1b8195ea6") {
		return false
	}
	return ta.custodyTargetClosed(arg)
}

func (ta *publicationGuardTaintAnalysis) custodyTargetClosed(expr ast.Expr) bool {
	refuse := func(format string, args ...any) bool {
		ta.custodyRefusal = " custody-proof-refused=" + fmt.Sprintf(format, args...)
		return false
	}
	if !ta.custodyAuxiliariesClosed() || ta.unknownTypes(expr) {
		return refuse("auxiliary or unknown target")
	}
	origins := ta.typesOf(expr)
	if len(origins) == 0 {
		return refuse("no retained target origins")
	}
	visited := make(map[types.Type]bool)
	decls := publicationGuardCustodyDecls(ta.source)
	var check func(types.Type) bool
	check = func(typ types.Type) bool {
		if visited[typ] {
			return true
		}
		visited[typ] = true
		key, digest, ok := publicationGuardCapsuleShape(ta.source, decls, typ)
		want := publicationGuardCustodyCertificates[key]
		if !ok || want == "" || want != digest {
			return refuse("uncertified type %s (%s)", types.TypeString(typ, publicationGuardQualifier), key)
		}
		named := types.Unalias(types.Unalias(typ).(*types.Pointer).Elem()).(*types.Named)
		shape := named.Underlying().(*types.Struct)
		opaqueNames := publicationGuardCustodyOpaqueFields[key]
		opaque := make(map[*types.Var]bool)
		for i := 0; i < shape.NumFields(); i++ {
			field := shape.Field(i)
			hidden := false
			for _, name := range opaqueNames {
				hidden = hidden || field.Name() == name
			}
			if hidden {
				opaque[field] = true
				continue
			}
			if ta.objectTaint[field]&(publicationGuardTaintState|publicationGuardTaintContainer) == 0 {
				continue
			}
			fieldType := field.Type()
			switch collection := fieldType.Underlying().(type) {
			case *types.Array:
				fieldType = collection.Elem()
			case *types.Slice:
				fieldType = collection.Elem()
			}
			if !types.Identical(fieldType, types.Universe.Lookup("error").Type()) || ta.unknownConcreteTypes[field] || len(ta.concreteTypes[field]) == 0 {
				return refuse("field %s.%s type=%s unknown=%v origins=%d", key, field.Name(), types.TypeString(fieldType, publicationGuardQualifier), ta.unknownConcreteTypes[field], len(ta.concreteTypes[field]))
			}
			for child := range ta.concreteTypes[field] {
				if !check(child) {
					return false
				}
			}
		}
		if !publicationGuardOpaqueFieldReadsClosed(ta.source, decls, opaque, publicationGuardCustodyReadObservers) {
			return refuse("payload read outside certified observer for %s", key)
		}
		return true
	}
	for typ := range origins {
		if !check(typ) {
			return false
		}
	}
	return true
}

// Keep every source certificate checked independently from whether a current
// production call happens to exercise it.
func TestPublicationSourceGuardCustodyCertificateInventory(t *testing.T) {
	source := loadProductionPublicationGuardSource(t)
	callbackBodies := make(map[*types.Func]bool)
	for _, file := range source.files {
		for _, declaration := range file.Decls {
			if declaration, ok := declaration.(*ast.FuncDecl); ok && declaration.Body != nil {
				if fn, ok := source.info.Defs[declaration.Name].(*types.Func); ok {
					callbackBodies[fn] = true
				}
			}
		}
	}
	decls := make(map[*types.Func]*ast.FuncDecl)
	named := make(map[string]types.Type)
	auxiliaryFunctions := make(map[string]string)
	observers := make(map[string]string)
	for _, file := range publicationGuardCustodyFiles(source) {
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if fn, ok := source.info.Defs[declaration.Name].(*types.Func); ok && declaration.Body != nil {
					decls[fn] = declaration
					if signature, ok := fn.Type().(*types.Signature); ok && signature.Recv() != nil {
						receiver := signature.Recv().Type()
						if ptr, ok := receiver.(*types.Pointer); ok {
							receiver = ptr.Elem()
						}
						if typ, ok := types.Unalias(receiver).(*types.Named); ok && typ.Obj().Pkg() != nil {
							named[typ.Obj().Pkg().Path()+"."+typ.Obj().Name()] = types.NewPointer(typ)
						}
					}
					if _, exists := publicationGuardCustodyAuxiliaryFunctions[publicationGuardFuncID(fn)]; exists {
						auxiliaryFunctions[publicationGuardFuncID(fn)] = fmt.Sprintf("%x", sha256.Sum256([]byte(publicationGuardRender(source.fset, declaration))))
					}
					if _, exists := publicationGuardCustodyReadObservers[publicationGuardFuncID(fn)]; exists {
						observers[publicationGuardFuncID(fn)] = fmt.Sprintf("%x", sha256.Sum256([]byte(publicationGuardRender(source.fset, declaration))))
					}
				}
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					if spec, ok := spec.(*ast.TypeSpec); ok {
						if object, ok := source.info.Defs[spec.Name].(*types.TypeName); ok && object.Pkg() != nil {
							named[object.Pkg().Path()+"."+object.Name()] = types.NewPointer(object.Type())
						}
					}
				}
			}
		}
	}
	for key, want := range publicationGuardCustodyAuxiliaryFunctions {
		got := auxiliaryFunctions[key]
		t.Logf("auxiliary-function %s %s", key, got)
		if got == "" || want == "" || got != want {
			t.Errorf("unreviewed auxiliary function %s", key)
		}
	}
	for key, want := range publicationGuardCustodyReadObservers {
		got := observers[key]
		t.Logf("observer %s %s", key, got)
		if got == "" || want == "" || got != want {
			t.Errorf("unreviewed observer %s", key)
		}
	}
	for key, want := range publicationGuardCustodyAuxiliaryTypes {
		_, got, ok := publicationGuardCapsuleShape(source, decls, named[key])
		t.Logf("auxiliary-type %s %s", key, got)
		if !ok || want == "" || got != want {
			t.Errorf("unreviewed auxiliary type %s", key)
		}
	}
	for key, want := range publicationGuardCustodyCertificates {
		_, got, ok := publicationGuardCapsuleShape(source, decls, named[key])
		t.Logf("capsule %s %s", key, got)
		if !ok || want == "" || want != got {
			t.Errorf("unreviewed capsule certificate %s", key)
			continue
		}
		typ := named[key].(*types.Pointer).Elem().(*types.Named)
		for i := 0; i < typ.NumMethods(); i++ {
			if !callbackBodies[typ.Method(i)] {
				t.Errorf("certified method missing normal body analysis: %s", publicationGuardFuncID(typ.Method(i)))
			}
		}
	}
}

// The certificate is invalidated by a new field or any method amendment. The
// independent field-read inventory also rejects a free accessor even though
// adding that function leaves the type/method digest unchanged.
func TestPublicationSourceGuardCustodyCertificateRejectsExposure(t *testing.T) {
	const original = `type privateErrorCapsule struct{payload *DBState;cause error}
func(e *privateErrorCapsule)Error()string{return "held"}
func(e *privateErrorCapsule)Unwrap()error{return e.cause}
`
	build := func(body string) (*publicationGuardSource, map[*types.Func]*ast.FuncDecl, types.Type, map[*types.Var]bool) {
		source := loadPublicationGuardFixture(t, body)
		decls := make(map[*types.Func]*ast.FuncDecl)
		for _, file := range source.files {
			for _, declaration := range file.Decls {
				if declaration, ok := declaration.(*ast.FuncDecl); ok {
					if fn, ok := source.info.Defs[declaration.Name].(*types.Func); ok && declaration.Body != nil {
						decls[fn] = declaration
					}
				}
			}
		}
		typ := types.NewPointer(source.pkg.Scope().Lookup("privateErrorCapsule").Type())
		field, _, _ := types.LookupFieldOrMethod(typ, true, source.pkg, "payload")
		return source, decls, typ, map[*types.Var]bool{field.(*types.Var): true}
	}
	source, decls, typ, opaque := build(original)
	_, want, ok := publicationGuardCapsuleShape(source, decls, typ)
	if !ok || !publicationGuardOpaqueFieldReadsClosed(source, decls, opaque, nil) {
		t.Fatal("private error-only surface rejected")
	}
	fixtures := map[string]struct {
		body                      string
		shapeChanged, readExposed bool
	}{
		"exported field":              {strings.Replace(original, "payload *DBState", "payload *DBState;State *DBState", 1), true, false},
		"exported accessor":           {original + `func(e *privateErrorCapsule)State()*DBState{return e.payload}`, true, true},
		"method body mutation":        {strings.Replace(original, `return "held"`, `e.payload.RootPageID++;return "held"`, 1), true, true},
		"bodyless method":             {strings.Replace(original, `Error()string{return "held"}`, `Error()string`, 1), true, false},
		"free accessor":               {original + `func expose(e *privateErrorCapsule)*DBState{return e.payload}`, false, true},
		"global accessor":             {original + `var exposedPayload=(&privateErrorCapsule{}).payload`, false, true},
		"slot alias":                  {original + `func expose(e *privateErrorCapsule)**DBState{return &e.payload}`, false, true},
		"assignment reads payload":    {original + `func expose(e *privateErrorCapsule){e.payload=e.payload}`, false, true},
		"original custody assignment": {original + `func preserve(e *privateErrorCapsule,state *DBState){e.payload=state}`, false, false},
	}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			source, decls, typ, opaque := build(fixture.body)
			_, got, valid := publicationGuardCapsuleShape(source, decls, typ)
			if changed := !valid || got != want; changed != fixture.shapeChanged {
				t.Fatalf("shape amendment=%v want %v", changed, fixture.shapeChanged)
			}
			if exposed := !publicationGuardOpaqueFieldReadsClosed(source, decls, opaque, nil); exposed != fixture.readExposed {
				t.Fatalf("payload read exposed=%v want %v", exposed, fixture.readExposed)
			}
		})
	}
	// A dependency accessor outside the callback-body corpus must still be
	// found in the complete checked source inventory.
	source, decls, typ, opaque = build(original + `func expose(e *privateErrorCapsule)*DBState{return e.payload}`)
	for fn, decl := range decls {
		if fn.Name() != "expose" {
			continue
		}
		delete(decls, fn)
		file := source.files[0]
		for i, declaration := range file.Decls {
			if declaration == decl {
				file.Decls = append(file.Decls[:i], file.Decls[i+1:]...)
				break
			}
		}
		source.custodyFiles = []*ast.File{{Decls: []ast.Decl{decl}}}
		break
	}
	if publicationGuardOpaqueFieldReadsClosed(source, decls, opaque, nil) {
		t.Fatal("dependency-only payload accessor accepted")
	}
	// Even the sole explicitly allowed observer is bound to its exact body.
	source, decls, typ, opaque = build(original + `func owns(e *privateErrorCapsule)bool{return e.payload!=nil}`)
	observers := make(map[string]string)
	for fn, decl := range decls {
		if fn.Name() == "owns" {
			observers[publicationGuardFuncID(fn)] = fmt.Sprintf("%x", sha256.Sum256([]byte(publicationGuardRender(source.fset, decl))))
		}
	}
	if !publicationGuardOpaqueFieldReadsClosed(source, decls, opaque, observers) {
		t.Fatal("exact bool observer rejected")
	}
	source, decls, _, opaque = build(original + `func owns(e *privateErrorCapsule)bool{e.payload.RootPageID++;return e.payload!=nil}`)
	if publicationGuardOpaqueFieldReadsClosed(source, decls, opaque, observers) {
		t.Fatal("observer body amendment accepted")
	}
}
