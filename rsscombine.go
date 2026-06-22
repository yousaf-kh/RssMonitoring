package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gorilla/feeds"
	"github.com/mmcdole/gofeed"
	"github.com/spf13/viper"
	"mvdan.cc/xurls"
)

func getURLsFromFeedsURL(feedsURL string) ([]string, error) {
	log.Printf("Loading feed URLs from: %v", feedsURL)
	client := &http.Client{
		Timeout: time.Duration(viper.GetInt("client_timeout_seconds")) * time.Second,
	}
	response, err := client.Get(feedsURL)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	stringContents := string(contents)
	// TODO: this is a hack
	for _, exclude := range viper.GetStringSlice("feed_exclude_prefixes") {
		stringContents = strings.Replace(stringContents, exclude, "", -1)
	}
	feedURLs := xurls.Strict.FindAllString(stringContents, -1)
	return feedURLs, nil
}

func getURLs() ([]string, error) {
	feedsURL := viper.GetString("feed_urls")
	if feedsURL != "" {
		return getURLsFromFeedsURL(feedsURL)
	}
	return viper.GetStringSlice("feeds"), nil
}

func fetchURL(url string, ch chan<- *gofeed.Feed) {
	log.Printf("Fetching URL: %v\n", url)
	fp := gofeed.NewParser()
	fp.Client = &http.Client{
		Timeout: time.Duration(viper.GetInt("client_timeout_seconds")) * time.Second,
	}
	feed, err := fp.ParseURL(url)
	if err == nil {
		ch <- feed
	} else {
		log.Printf("Error on URL: %v (%v)", url, err)
		ch <- nil
	}
}

func fetchURLs(urls []string) []*gofeed.Feed {
	allFeeds := make([]*gofeed.Feed, 0)
	ch := make(chan *gofeed.Feed)
	for _, url := range urls {
		go fetchURL(url, ch)
	}
	for range urls {
		feed := <-ch
		if feed != nil {
			allFeeds = append(allFeeds, feed)
		}
	}
	return allFeeds
}

// TODO: there must be a shorter syntax for this
type byPublished []*gofeed.Feed

func (s byPublished) Len() int {
	return len(s)
}

func (s byPublished) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byPublished) Less(i, j int) bool {
	date1 := feedDate(s[i])
	date2 := feedDate(s[j])
	if date1 == nil && date2 == nil {
		return false
	}
	if date1 == nil {
		return true
	}
	if date2 == nil {
		return false
	}
	return date1.Before(*date2)
}

func feedDate(f *gofeed.Feed) *time.Time {
	if len(f.Items) == 0 {
		return nil
	}
	if f.Items[0].PublishedParsed != nil {
		return f.Items[0].PublishedParsed
	}
	return f.Items[0].UpdatedParsed
}

func getAuthor(feed *gofeed.Feed) string {
	if feed.Author != nil {
		return feed.Author.Name
	}
	if len(feed.Items) > 0 && feed.Items[0].Author != nil {
		return feed.Items[0].Author.Name
	}
	log.Printf("Could not determine author for %v", feed.Link)
	return viper.GetString("default_author_name")
}

func combineAllFeeds(allFeeds []*gofeed.Feed) *feeds.Feed {
	feed := &feeds.Feed{
		Title:       viper.GetString("title"),
		Link:        &feeds.Link{Href: viper.GetString("link")},
		Description: viper.GetString("description"),
		Author: &feeds.Author{
			Name:  viper.GetString("author_name"),
			Email: viper.GetString("author_email"),
		},
		Created: time.Now(),
	}
	sort.Sort(sort.Reverse(byPublished(allFeeds)))
	limitPerFeed := viper.GetInt("feed_limit_per_feed")
	seen := make(map[string]bool)
	for _, sourceFeed := range allFeeds {
		for i, item := range sourceFeed.Items {
			if i >= limitPerFeed {
				break
			}
			if item == nil {
				continue
			}
			if seen[item.Link] {
				continue
			}
			created := item.PublishedParsed
			if created == nil {
				created = item.UpdatedParsed
			}
			if created == nil {
				now := time.Now()
				created = &now
			}
			feed.Items = append(feed.Items, &feeds.Item{
				Title:       item.Title,
				Link:        &feeds.Link{Href: item.Link},
				Description: item.Description,
				Author:      &feeds.Author{Name: getAuthor(sourceFeed)},
				Created:     *created,
				Content:     item.Content,
			})
			seen[item.Link] = true
		}
	}
	return feed
}

func GetAtomFeed() (*feeds.Feed, error) {
	urls, err := getURLs()
	if err != nil {
		return nil, err
	}
	allFeeds := fetchURLs(urls)
	return combineAllFeeds(allFeeds), nil
}

func LoadConfig() {
	viper.SetConfigName("rsscombine")
	viper.AddConfigPath(".")
	viper.SetEnvPrefix("RSSCOMBINE")
	viper.AutomaticEnv()
	viper.SetDefault("default_author_name", "Unknown Author")
	viper.SetDefault("client_timeout_seconds", "60")
	viper.SetDefault("feed_limit_per_feed", "20")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
}

func runServer(port string) {
	cacheTimeout := time.Duration(viper.GetInt("cache_timeout_seconds")) * time.Second
	var (
		mu          sync.Mutex
		cachedAtom  string
		cacheExpiry time.Time
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if time.Now().After(cacheExpiry) || cachedAtom == "" {
			feed, err := GetAtomFeed()
			if err != nil {
				log.Printf("Error fetching feeds: %v", err)
				http.Error(w, "Failed to fetch feeds", http.StatusInternalServerError)
				return
			}
			atom, err := feed.ToAtom()
			if err != nil {
				log.Printf("Error rendering feed: %v", err)
				http.Error(w, "Failed to render feed", http.StatusInternalServerError)
				return
			}
			cachedAtom = atom
			cacheExpiry = time.Now().Add(cacheTimeout)
			log.Printf("Cache refreshed with %v items, next refresh in %v", len(feed.Items), cacheTimeout)
		}

		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		fmt.Fprint(w, cachedAtom)
	})

	log.Printf("Starting server on http://localhost:%v", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func main() {
	LoadConfig()

	port := viper.GetString("port")
	if port != "" {
		runServer(port)
		return
	}

	bucket := viper.GetString("s3_bucket")
	filename := viper.GetString("s3_filename")
	combinedFeed, err := GetAtomFeed()
	if err != nil {
		log.Fatalf("Failed to get atom feed: %v", err)
	}
	atom, err := combinedFeed.ToAtom()
	if err != nil {
		log.Fatalf("Failed to render atom feed: %v", err)
	}
	log.Printf("Rendered RSS with %v items", len(combinedFeed.Items))
	if len(bucket) == 0 {
		fmt.Print(atom)
		return
	}
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(filename),
		Body:        strings.NewReader(atom),
		ContentType: aws.String("text/xml"),
		ACL:         aws.String("public-read"),
	})
	if err != nil {
		log.Fatalf("Unable to upload %q to %q, %v", filename, bucket, err)
	}
	log.Printf("Successfully uploaded %q to %q\n", filename, bucket)
}
