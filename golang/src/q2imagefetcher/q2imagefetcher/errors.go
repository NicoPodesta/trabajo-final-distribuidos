package q2imagefetcher

import "errors"

var (
	errImageSource = errors.New("image source failure")
	errImageLocal  = errors.New("local image I/O failure")
)

func isImageSourceError(err error) bool {
	return errors.Is(err, errImageSource) && !errors.Is(err, errImageLocal)
}
