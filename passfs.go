package ifs

type Passfs struct {
	Paths map[string] string
}

func (pfs *Passfs) ShouldPass(path string) bool {
	_, ok := pfs.Paths[path]
	return ok
}

func (pfs *Passfs) Attr()  {

}

func (pfs *Passfs) Readdir() {

}
