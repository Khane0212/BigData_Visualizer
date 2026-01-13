# visualizer.py
import plotly.express as px
from wordcloud import WordCloud

def update_chart_layout(fig):
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Arial", size=12),
        margin=dict(l=10, r=10, t=40, b=10) 
    )
    return fig

# BIỂU ĐỒ XU HƯỚNG
def plot_articles_over_time(df):
    if df is None or df.empty: return None
    
    fig = px.bar(
        df, 
        x="year", 
        y="count", 
        title="Xu hướng bài viết theo năm", 
        color="count", 
        color_continuous_scale="Blues",
        text_auto=True 
    )
    fig.update_traces(textposition='outside')
    return update_chart_layout(fig)

# TOP 10 DÀI NHẤT
def plot_top_10_longest(df):
    if df is None or df.empty: return None
    
    fig = px.bar(
        df, 
        x="word_count", 
        y="title", 
        orientation='h', 
        title="Top 10 bài dài nhất",
        text='word_count',
        color='word_count',
        color_continuous_scale='Viridis' 
    )
    
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    fig.update_traces(textposition='inside') 
    
    return update_chart_layout(fig)

# PHÂN BỐ 
def plot_word_count_distribution(df, show_outliers=False):
    if df is None or df.empty: return None
    
    data_to_plot = df.copy()
    title = "Phân bố độ dài (Log Scale)"
    use_log = False 
    
    if not show_outliers:
        data_to_plot = data_to_plot[data_to_plot["word_count"] < 5000]
        title = "Phân bố độ dài (< 5000 từ)"
    else:
        use_log = True 

    if data_to_plot.empty: return None

    fig = px.histogram(
        data_to_plot, 
        x="word_count", 
        nbins=50,
        title=title,
        color_discrete_sequence=['#00CC96'], 
        log_y=use_log
    )

    if use_log:
        fig.update_layout(
            yaxis=dict(
                tickmode='array', 
                tickvals=[1, 10, 100, 1000, 10000], 
                ticktext=['1', '10', '100', '1k', '10k'] 
            )
        )
    
    fig.update_layout(bargap=0.1)
    return update_chart_layout(fig)

# WORDCLOUD
def generate_wordcloud(df):
    if df is None or df.empty: return None
    text = " ".join(df["text_clean"].dropna().astype(str))
    if not text: return None

    wc = WordCloud(
        width=800, 
        height=400, 
        background_color="white", 
        colormap="viridis",
        max_words=200
    ).generate(text)
    
    return wc.to_array()