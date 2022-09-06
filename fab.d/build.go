package build

import (
	"os"
	"path/filepath"

	"github.com/bobg/fab"
)

var Gen = fab.Seq(
	proto("anchor"),
	proto("fs"),
	proto("schema"),
	proto("split"),
	proto("store/rpc"),
)

var Test = fab.Deps(
	&fab.Command{
		Shell:  "go test ./...",
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	},
	Gen,
)

var Build = fab.Deps(
	&fab.Command{Shell: "go build ./cmd/bs"},
	Gen,
)

var Vet = &fab.Command{
	Shell:  "go vet ./...",
	Stdout: os.Stdout,
	Stderr: os.Stderr,
}

var Lint = &fab.Command{
	Shell:  "staticcheck ./...",
	Stdout: os.Stdout,
	Stderr: os.Stderr,
}

var Check = fab.Seq(Vet, Lint, Test)

func proto(dir string) fab.Target {
	var (
		basename  = filepath.Base(dir)
		protoname = basename + ".proto"
	)
	return fab.FilesTarget{
		Target: &fab.Command{
			Shell: "protoc -I. --go_out=. " + protoname,
			Dir:   dir,
		},
		In:  []string{filepath.Join(dir, protoname)},
		Out: []string{filepath.Join(dir, basename+".pb.go")},
	}
}
