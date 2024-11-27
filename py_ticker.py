import tickertick as tt
import tickertick.query as query

feed = tt.get_feed(
    query = query.And(
        query.BroadTicker('aapl'),
        query.StoryType(query.StoryTypes.SEC)
    )
) # SEC filings from Apple Inc.

print(feed)

# Process and print key information from each SEC filing
for story in feed:
    print(f"\nFiling Time: {story.time}")
    print(f"Filing URL: {story.url}")
    print(f"Filing ID: {story.id}")
    if story.similar_stories:
        print(f"Similar Filings: {len(story.similar_stories)}")
