package remote

import (
	"fmt"
	"net/url"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TopicPath returns "/v1/topics/<topic>/<suffix...>" under the remote's
// base path, after checking topic against topic.ValidateName, with every
// segment through url.PathEscape. suffix is a constant such as
// "produce", "batch" or "children".
//
// The name rule already makes a topic one path-safe segment (no "/",
// no "%", never "." or ".."), so no value can move a credentialed
// request to another route on the remote's host; the escaping is the
// second line of that defence.
func TopicPath(topicName string, suffix ...string) (string, error) {
	if err := topic.ValidateName(topicName); err != nil {
		return "", fmt.Errorf("remote topic: %w", err)
	}
	p := "/v1/topics/" + url.PathEscape(topicName)
	for _, s := range suffix {
		if s == "" || s == "." || s == ".." {
			return "", fmt.Errorf("remote path: invalid segment")
		}
		p += "/" + url.PathEscape(s)
	}
	return p, nil
}

// UsersPath returns "/v1/users" (check 7).
func UsersPath() string { return "/v1/users" }
