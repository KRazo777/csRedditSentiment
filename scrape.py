from praw import Reddit
from prawcore.exceptions import NotFound, TooManyRequests
from praw.models import Subreddit, Submission
from praw.models.comment_forest import CommentForest
import os
import time
import pandas as pd
from dotenv import load_dotenv

SUBREDDIT_NAMES = [
    # Original list
    'cscareerquestions',
    'cscareerquestionsEU',
    'csmajors',
    'cscareers',
    'recruitinghell',
    'experienceddevs',
    'artificialinteligence',
    'singularity',
    'compsci',
    'computerscience',
    'jobs',

    # Career & Job Market / CS Majors
    'TechCareerAdvice',
    'ITCareerQuestions',
    'JobHunting',
    'Internships',
    'resumes',
    'AskEngineers',

    # AI & Industry Trends
    'MachineLearning',
    'DeepLearning',
    'ArtificialInteligence',
    'OpenAI',
    'ChatGPT',

    # Computer Science / Developer Communities
    'Programming',
    'learnprogramming',
    'WebDev',
]


KEYWORD_QUERIES: list[str] = [
  'ai',
  'internship',
  'job',
  'career',
  'chatgpt',
  'industry'
]

POST_OUTPUT_FILE_NAME: str = 'posts.jsonl'
COMMENT_OUTPUT_FILE_NAME: str = 'comments.jsonl'
NUM_TOP_COMMENTS = 45

def init_reddit_instance() -> Reddit:
  load_dotenv()

  return Reddit(
    client_id = os.getenv("REDDIT_CLIENT_ID"),
    client_secret = os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent = os.getenv("REDDIT_USER_AGENT"),
    ratelimit_seconds=60
  )

def scrape_post_comments(comment_batch: list[dict], post: Submission):
  post.comments.replace_more(limit=0)

  for comment in post.comments[:NUM_TOP_COMMENTS]:
    comment_data = {
      'comment_id': comment.id,
      'post_id': post.id,
      'body': comment.body,
      'date_created': comment.created_utc,
      'score': comment.score
    }

    comment_batch.append(comment_data)

def main():
  if os.path.exists(POST_OUTPUT_FILE_NAME):
    print('Clearing reddit_posts.jsonl')
    os.remove(POST_OUTPUT_FILE_NAME)

  if os.path.exists(COMMENT_OUTPUT_FILE_NAME):
    print('Clearing reddit_comments.jsonl')
    os.remove(COMMENT_OUTPUT_FILE_NAME)


  print("Obtaining Reddit Instance")
  reddit: Reddit = init_reddit_instance()

  post_batch: list[dict] = []
  comment_batch: list[dict] = []

  seen_post_ids: set = set()

  total_posts_scraped = 0

  for subreddit_name in SUBREDDIT_NAMES:
    subreddit: Subreddit = reddit.subreddit(subreddit_name)
    posts_scraped = 0
    
    for query in KEYWORD_QUERIES:
      print(f'Scraping posts from r/{subreddit_name} with query "{query}"')
      try:
        for submission in subreddit.search(query, sort='comments', time_filter='all', limit=None):

          if submission.id in seen_post_ids:
            continue

          seen_post_ids.add(submission.id)

          post = {
            'post_id': submission.id,
            'title': submission.title,
            'body': submission.selftext,
            'subreddit': submission.subreddit.display_name,
            'date_created': submission.created_utc,
            'total_upvotes': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'num_comments': submission.num_comments,
            'url': submission.url
          }

          post_batch.append(post)
          posts_scraped += 1

          if len(post_batch) >= 200:
            pd.DataFrame(post_batch).to_json(POST_OUTPUT_FILE_NAME, orient='records',
                                        lines=True, mode = 'a', force_ascii=False)

            post_batch.clear()

          scrape_post_comments(comment_batch, submission)

          if len(comment_batch) >= 200:
            pd.DataFrame(comment_batch).to_json(COMMENT_OUTPUT_FILE_NAME, orient='records',
                                        lines=True, mode = 'a', force_ascii=False)

            comment_batch.clear()

      except NotFound:
        print('Not Found')

      except TooManyRequests as e:
        wait = int(e.response.headers.get("Retry-After", 5))
        print(f"Rate limited. Waiting {wait} seconds.")
        time.sleep(wait)

    print(f'Scraped {posts_scraped} posts from r/{subreddit_name}')
    total_posts_scraped += posts_scraped

  print(f'{total_posts_scraped} total posts scraped!')

if __name__ == '__main__':
  main()
