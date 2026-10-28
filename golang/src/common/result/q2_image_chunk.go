package result

import (
	"fmt"
	"math"
)

// TODO: consultar tamaño al profe
const DefaultQ2ChunkSize = 64 * 1024

// Name y Url corresponden al artículo y solo viajan cuando Seq es 0.
// Last indica que termina de enviarse la imagen
type Q2ImageChunk struct {
	ImageID string `json:"image_id"`
	Seq     uint32 `json:"seq"`
	Last    bool   `json:"last"`
	Bytes   []byte `json:"bytes"`
	Name    string `json:"name,omitempty"`
	Url     string `json:"url,omitempty"`
}

// El orden entre chunks lo valida el receptor
func (c *Q2ImageChunk) Validate() error {
	if c == nil {
		return fmt.Errorf("nil Q2 image chunk")
	}
	if c.ImageID == "" {
		return fmt.Errorf("Q2 image chunk requires image_id")
	}
	if len(c.Bytes) == 0 {
		return fmt.Errorf("Q2 image chunk requires non-empty bytes, including the last chunk")
	}
	if c.Seq != 0 && (c.Name != "" || c.Url != "") {
		return fmt.Errorf("Q2 image chunk metadata is only allowed at seq=0")
	}

	for _, field := range []struct {
		name string
		size int
	}{
		{"image_id", len(c.ImageID)},
		{"name", len(c.Name)},
		{"url", len(c.Url)},
		{"bytes", len(c.Bytes)},
	} {
		if uint64(field.size) > math.MaxUint32 {
			return fmt.Errorf("Q2 image chunk %s exceeds uint32 length", field.name)
		}
	}
	return nil
}
