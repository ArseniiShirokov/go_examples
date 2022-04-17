package externalsort

import (
	"container/heap"
	"io"
	"os"
	"sort"
	"strings"
)

type item struct {
	s      string
	fileID int
}

type IntHeap []item

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i].s < h[j].s }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(item))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type LineR struct {
	r io.Reader
}

type LineW struct {
	w io.Writer
}

func (l *LineR) ReadLine() (string, error) {
	var sb strings.Builder

	buf := make([]byte, 1)
	n, err := l.r.Read(buf)
	if err != nil && err != io.EOF {
		return "", err
	}
	for err != io.EOF && buf[0] != '\n' {
		sb.Write(buf)
		n, err = l.r.Read(buf)
		if err != nil && err != io.EOF {
			return "", err
		}
	}
	if err == io.EOF && n == 1 {
		sb.Write(buf)
	}
	return sb.String(), err
}

func (l *LineW) Write(s string) error {
	buf := []byte(s)
	enter := make([]byte, 1)
	enter[0] = '\n'
	_, err := l.w.Write(buf)
	if err != nil {
		return err
	}
	_, err = l.w.Write(enter)
	return err
}

func NewReader(r io.Reader) LineReader {
	return &LineR{r}
}

func NewWriter(w io.Writer) LineWriter {
	return &LineW{w}
}

func Merge(w LineWriter, readers ...LineReader) error {
	var items []item
	for i, reader := range readers {
		s, err := reader.ReadLine()
		if err != nil && err != io.EOF {
			return err
		} else if err != io.EOF || len(s) > 0 {
			items = append(items, item{s, i})
		}
	}

	h := IntHeap(items)
	heap.Init(&h)
	for h.Len() > 0 {
		it := heap.Pop(&h)
		err := w.Write(it.(item).s)
		if err != nil {
			return err
		}

		s, err := readers[it.(item).fileID].ReadLine()
		if err != nil && err != io.EOF {
			return err
		} else if err != io.EOF || len(s) > 0 {
			heap.Push(&h, item{s, it.(item).fileID})
		}
	}
	return nil
}

func SortOne(filename string) error {
	var _ io.Reader = (*os.File)(nil)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	r := NewReader(f)

	var lines []string
	s, err := r.ReadLine()
	if err != nil && err != io.EOF {
		return err
	}
	for err != io.EOF || len(s) > 0 {
		lines = append(lines, s)
		s, err = r.ReadLine()
		if err != nil && err != io.EOF {
			return err
		}
	}

	defer f.Close()
	sort.Strings(lines)

	var _ io.Writer = (*os.File)(nil)
	outF, err := os.OpenFile(filename, os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	w := NewWriter(outF)
	for _, line := range lines {
		err = w.Write(line)
		if err != nil {
			return err
		}
	}
	defer outF.Close()

	return nil
}

func Sort(w io.Writer, in ...string) error {
	var _ io.Reader = (*os.File)(nil)
	var readers []LineReader
	for _, filename := range in {
		err := SortOne(filename)
		if err != nil {
			return err
		}

		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		r := NewReader(f)

		readers = append(readers, r)
	}
	err := Merge(NewWriter(w), readers...)
	return err
}
