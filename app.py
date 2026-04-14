import streamlit as st
import pandas as pd
import numpy as np
from spark_functions import init_spark_session, load_mysql_dataframe

# Requires: streamlit package (pip install streamlit)
# To run: streamlit run app.py

# DATAFRAME SETUP

DATABASE_TABLE_NAME: str = 'reddit_posts'
spark = init_spark_session()
df: pd.DataFrame = load_mysql_dataframe(spark, DATABASE_TABLE_NAME).toPandas()
df['post_date'] = pd.to_datetime(df['post_date'])
df = df[df['post_date'] >= "2019-01-01"]

def main() -> None:
  st.set_page_config('Reddit Sentiment Analysis Dashboard', layout='wide')

  # ACTUAL PAGE STUFF STARTS HERE
  st.markdown('# Reddit Sentiment Analysis Dashboard')

  DEFAULT_KEYWORDS = ['internship', 'ai']

  col1, col2 = st.columns([2, 3])

  # Multiselect to select which keywords are currently active

  selected_keywords = st.multiselect(
      "Keywords / Phrases",
      options=DEFAULT_KEYWORDS,           # initial options
      default=DEFAULT_KEYWORDS,           # pre-selected
      max_selections=6,
      accept_new_options=True             # allow adding new keywords
  )

  st.write('## Average Sentiment By Keyword')

  # Only plot if there is at least one keyword selected
  if selected_keywords:
      def avg_sentiment_for_keywords(df, keywords):
          rows = []

          for kw in keywords:
              mask = (
                  df['full_text'].str.contains(kw, case=False, na=False)
              )

              avg = df[mask]['sentiment_score'].mean()
              rows.append({"keyword": kw, "avg_sentiment": avg})

          return pd.DataFrame(rows)

      chart_data = avg_sentiment_for_keywords(df, selected_keywords)
      st.bar_chart(chart_data.set_index('keyword'), horizontal=True) 
  else:
     st.markdown('No Keywords Selected')
  
  st.header('Sentiment Trends Over Time')

  if selected_keywords:

    df['month'] = df['post_date'].dt.to_period('M')

    def get_keywords(row):
        text = f"{row['full_text']}".lower()
        matched = [kw for kw in selected_keywords if kw.lower() in text]
        return matched if matched else np.nan

    df['matched_keywords'] = df.apply(get_keywords, axis=1)

    df_exploded = df.dropna(subset=['matched_keywords']).explode('matched_keywords')

    avg_sentiment = (
        df_exploded
        .groupby(['month', 'matched_keywords'])['sentiment_score']
        .mean()
        .reset_index()
    )
    
    avg_sentiment['month'] = avg_sentiment['month'].dt.to_timestamp()

    avg_sentiment_pivot = avg_sentiment.pivot(
        index='month', columns='matched_keywords', values='sentiment_score'
    ).fillna(0)

    st.line_chart(avg_sentiment_pivot)

    st.markdown('---')
    st.write('### "AI" Post Frequency Trend (2020-2025)')
  
    df_ai = df[df['full_text'].str.contains('ai', case=False, na=False)].copy()
    
    df_ai['post_date'] = pd.to_datetime(df_ai['post_date'])
    df_ai = df_ai[
        (df_ai['post_date'] >= '2020-01-01') & 
        (df_ai['post_date'] <= '2025-10-30')
    ].copy()
    
    df_ai['month'] = df_ai['post_date'].dt.to_period('M')

    df_ai_trend = df_ai.groupby('month').size().reset_index(name='ai_post_count')
    
    df_ai_trend['post_date'] = df_ai_trend['month'].dt.to_timestamp()
    df_ai_trend = df_ai_trend.set_index('post_date')
    
    st.line_chart(
        df_ai_trend, 
        y='ai_post_count', 
        x_label='Date', 
        y_label='Monthly Post Count (AI)',
        use_container_width=True
    )
    st.markdown("---")

    st.subheader("Post Distribution per Subreddit by Keyword")
    n_cols = 3  
    for i in range(0, len(selected_keywords), n_cols):
        cols = st.columns(n_cols)
        for j, kw in enumerate(selected_keywords[i:i+n_cols]):
            df_kw = df[df['full_text'].str.contains(kw, case=False, na=False)]
            with cols[j]:
                st.markdown(f"**'{kw}'**")
                if not df_kw.empty:
                    post_counts = (
                        df_kw.groupby('subreddit')
                        .size()
                        .reset_index(name='total_posts')
                        .sort_values('total_posts', ascending=False)
                    )
                    st.bar_chart(post_counts.set_index('subreddit'),
                                 horizontal=True,
                                 sort='-total_posts')
                else:
                    st.write("No posts found for this keyword.")
    
    st.markdown("---")

    st.subheader("Average Sentiment per Keyword by Subreddit")
    n_cols = 3  
    
    for i in range(0, len(selected_keywords), n_cols):
        cols = st.columns(n_cols)
        for j, kw in enumerate(selected_keywords[i:i+n_cols]):
            
            df_kw = df[df['full_text'].str.contains(kw, case=False, na=False)].copy()
            
            with cols[j]:
                st.markdown(f"**Sentiment for '{kw}'**")
                
                if not df_kw.empty:
                    sentiment_agg = (
                        df_kw.groupby('subreddit')['sentiment_score']
                        .mean()
                        .reset_index(name='avg_sentiment')
                        .sort_values('avg_sentiment', ascending=False)
                    )
                    
                    st.bar_chart(sentiment_agg.set_index('subreddit'),
                                 horizontal=True,
                                 y='avg_sentiment',
                                 sort='-avg_sentiment')
                else:
                    st.write("No posts found for this keyword.")
    
    


if __name__ == '__main__':
  main()