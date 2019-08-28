# twitter-stats

To run, the following command line arguments are required:
```
--apiKey foo
--apiSecretKey bar
--accessToken baz
--accessTokenSecret qux
```

Once running, read statistics at http://localhost:8080/twitterStats?seconds=600&list_limit=10

The following statistics are provided for the requested time window:

* number of tweets 
* hashtags
  * count of each hashtag seen
* urls
  * count of each domain seen
  * number of tweets with a URL
  * percentage of  tweets with a URL
* emojis
  * count of each emoji seen
  * number of tweets with an emoji
  * percentage of tweets with an emoji

Additionally, the following fields are provided to indicate the actual time window that was used:
* seconds elapsed
* timestamp of the earliest tweet in the window
* timestmap of the latest tweet in the window
