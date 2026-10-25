package result

type Q1Result struct {
	Url          string
	DateModified uint32
}

type Q2Result struct {
	Name  string
	Url   string
	Image []byte
}

type Q3Result struct {
	Word string
}

type Q4Result struct {
	Word  string
	Count uint32
}

type Q5Result struct {
	Url       string
	WordCount uint32
}
