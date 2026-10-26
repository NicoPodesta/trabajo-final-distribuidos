package result

type Q1Result struct {
	Url          string `json:"url"`
	DateModified uint32 `json:"date_modified"`
}

type Q2Result struct {
	Name  string `json:"name"`
	Url   string `json:"url"`
	Image []byte `json:"image"`
}

type Q3Result struct {
	Word string `json:"word"`
}

type Q4Result struct {
	Word  string `json:"word"`
	Count uint32 `json:"count"`
}

type Q5Result struct {
	Url       string `json:"url"`
	WordCount uint32 `json:"word_count"`
}
