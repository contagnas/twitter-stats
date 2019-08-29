# twitter-stats
​
### Run
​
To run, the following command line arguments are required:
```
--apiKey foo 
--apiSecretKey bar 
--accessToken baz 
--accessTokenSecret qux
```

### Statistics
​
Once running, a service will spawn providing tweet statistics:
​

http://localhost:8080/twitterStats?seconds=600&listLimit=10

​
The following statistics are provided for the requested time window:
​
* Number of tweets 
* Hashtags
  * Count of each hashtag seen
* URLs
  * Count of each domain seen
  * Number of tweets with a URL
  * Percentage of  tweets with a URL
* Emojis
  * Count of each emoji seen
  * Number of tweets with an emoji
  * Percentage of tweets with an emoji
​
Additionally, the following fields are provided to indicate the actual time window that was used:
* Seconds elapsed
* Timestamp of the earliest tweet in the window
* Timestamp of the latest tweet in the window
