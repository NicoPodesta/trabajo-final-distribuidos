package q2filter

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
)

func (filter *Filter) handleArticlesMessage(articleMsg *inner.Q2ArticlesMessage) error {
	for index, article := range articleMsg.Records {
		requestID := strconv.FormatUint(articleMsg.BatchID, 10) + ":" + strconv.Itoa(index)
		if !matchesArticle(article) {
			continue
		}
		if strings.TrimSpace(article.Image) == "" {
			slog.Debug("Skipping Q2 article without image", "clientID", articleMsg.ClientID, "url", article.Url)
			continue
		}
		if err := filter.publishImageDownload(articleMsg.ClientID, requestID, article); err != nil {
			return err
		}
	}
	return nil
}

func matchesArticle(article inner.Q2ArticleItem) bool {
	if time.Unix(int64(article.DateModified), 0).UTC().Year() > 2020 {
		return false
	}
	for _, section := range article.Sections {
		section = strings.TrimSpace(section)
		if strings.EqualFold(section, "biography") || strings.EqualFold(section, "biographie") {
			return true
		}
	}
	return false
}
