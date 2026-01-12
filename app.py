import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from wordcloud import WordCloud

st.set_page_config(page_title="Trực quan hóa Big Data", layout="wide")
st.title("BigData Visualizer: Phân tích Wikipedia")
st.markdown("Dashboard phân tích dữ liệu Wikipedia tiếng Việt từ MongoDB Cloud.")

@st.cache_resource
def init_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        return MongoClient(uri)
    except Exception as e:
        st.error(f"Lỗi kết nối MongoDB: {e}")
        return None

client = init_connection()

st.sidebar.header("Nguồn dữ liệu")

if client:
    try:
        all_dbs = client.list_database_names()
        dbs = [db for db in all_dbs if db not in ['admin', 'local', 'config']]
        
        if not dbs:
            st.error("Cluster rỗng (Không có Database)!")
            st.stop()
            
        selected_db = st.sidebar.selectbox("Chọn Database", dbs)
        
        db = client[selected_db]
        cols = db.list_collection_names()
        selected_col = st.sidebar.selectbox("Chọn Collection (Bảng)", cols)
        
        limit_rows = st.sidebar.slider("Giới hạn số dòng tải về", 100, 10000, 2000)
        
        if st.sidebar.button("Tải dữ liệu"):
            st.cache_data.clear()

    except Exception as e:
        st.error(f"Lỗi kết nối: {e}")
        st.stop()
else:
    st.stop()

@st.cache_data(ttl=600)
def load_data(db_name, col_name, limit):
    try:
        collection = client[db_name][col_name]
        projection = {
            "_id": 0, "title": 1, "rev_ts": 1, 
            "text_len": 1, "word_count": 1, "text_clean": 1
        }

        cursor = collection.find({}, projection).sort("rev_ts", -1).limit(limit)
        return pd.DataFrame(list(cursor))
    except Exception as e:
        st.error(f"Lỗi truy vấn: {e}")
        return pd.DataFrame()

if selected_db and selected_col:
    with st.spinner("Đang tải dữ liệu từ Cloud về..."):
        df = load_data(selected_db, selected_col, limit_rows)

    if not df.empty:
        if "rev_ts" in df.columns:
            df["timestamp"] = pd.to_datetime(df["rev_ts"], errors='coerce')
            df["year"] = df["timestamp"].dt.year
            df["hour"] = df["timestamp"].dt.hour

            day_map = {
                'Monday': 'Thứ 2', 'Tuesday': 'Thứ 3', 'Wednesday': 'Thứ 4',
                'Thursday': 'Thứ 5', 'Friday': 'Thứ 6', 'Saturday': 'Thứ 7', 'Sunday': 'CN'
            }
            df["day_of_week"] = df["timestamp"].dt.day_name().map(day_map)

        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng bài viết", f"{len(df):,}")
        
        if "word_count" in df.columns:
            c2.metric("Số từ trung bình", f"{int(df['word_count'].mean())}")
        if "text_len" in df.columns:
            c3.metric("Độ dài lớn nhất (ký tự)", f"{df['text_len'].max():,}")
        c4.metric("Chủ đề duy nhất", f"{df['title'].nunique()}")
        st.divider()

        tab1, tab2, tab3, tab4 = st.tabs([
            "1. Tổng quan", 
            "2. Phân tích sâu", 
            "3. Theo thời gian",
            "4. Nội dung & Dữ liệu"
        ])

        with tab1:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Số lượng bài viết theo Năm")
                if "year" in df.columns:
                    year_counts = df["year"].value_counts().sort_index().reset_index()
                    year_counts.columns = ["Năm", "Số lượng"]
                    
                    fig_year = px.bar(year_counts, x="Năm", y="Số lượng", 
                                      color="Số lượng", color_continuous_scale="Reds")
                    st.plotly_chart(fig_year, use_container_width=True)

            with col2:
                st.subheader("Top 10 bài viết dài nhất")
                if "title" in df.columns and "word_count" in df.columns:
                    top_10 = df.nlargest(10, 'word_count').sort_values('word_count', ascending=True)
                    
                    fig_top = px.bar(top_10, x="word_count", y="title", orientation='h',
                                     labels={'word_count': 'Số lượng từ', 'title': 'Tên bài'},
                                     text="word_count", color="word_count", color_continuous_scale="Viridis")
                    st.plotly_chart(fig_top, use_container_width=True)

        with tab2:
            col3, col4 = st.columns(2)

            with col3:
                st.subheader("Tương quan: Độ dài vs Số từ")
                if "text_len" in df.columns and "word_count" in df.columns:
                    fig_scat = px.scatter(df, x="word_count", y="text_len", 
                                          labels={'word_count': 'Số lượng từ', 'text_len': 'Độ dài ký tự'},
                                          color="word_count", hover_data=["title"],
                                          title="Zoom vào để xem các điểm ngoại lai")
                    st.plotly_chart(fig_scat, use_container_width=True)

            with col4:
                st.subheader("Phân bổ độ dài bài viết")
                if "word_count" in df.columns:
                    fig_hist = px.histogram(df, x="word_count", nbins=50,
                                            labels={'word_count': 'Số lượng từ'},
                                            title="Phân bố thực tế của toàn bộ dữ liệu",
                                            color_discrete_sequence=['skyblue'])
                    
                    fig_hist.update_layout(bargap=0.1)

                    st.plotly_chart(fig_hist, use_container_width=True)

            st.subheader("Phân bố & Ngoại lai")
            if "word_count" in df.columns:
                fig_violin = px.violin(df, x="word_count", box=True, points="all",
                                       labels={'word_count': 'Số lượng từ'},
                                       title="Chỗ phình to là đa số bài viết, Chấm lẻ tẻ là bài ngoại lai",
                                       color_discrete_sequence=['#00CC96'],
                                       hover_data=["title"])
                
                fig_violin.update_layout(hovermode="x unified")
                st.plotly_chart(fig_violin, use_container_width=True)

                st.caption("Danh sách các bài viết dài nhất (Top 1%):")
                
                threshold = df["word_count"].quantile(0.99)
                outliers = df[df["word_count"] > threshold].sort_values("word_count", ascending=False)

                st.dataframe(
                    outliers[["title", "word_count", "text_len", "year"]],
                    use_container_width=True,
                    column_config={
                        "title": "Tên bài viết",
                        "word_count": st.column_config.NumberColumn("Số từ", format="%d từ"),
                        "text_len": "Độ dài (ký tự)",
                        "year": "Năm"
                    }
                )

        with tab3:
            st.subheader("Mật độ hoạt động chi tiết")
            if "day_of_week" in df.columns and "hour" in df.columns:
                try:
                    matrix = df.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)

                    thu_tu_tuan = ['Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7', 'CN']

                    matrix = matrix.reindex([d for d in thu_tu_tuan if d in matrix.index])

                    for h in range(24):
                        if h not in matrix.columns:
                            matrix[h] = 0
                    matrix = matrix.sort_index(axis=1)

                    fig_heat = px.imshow(
                        matrix,
                        labels=dict(x="Giờ trong ngày", y="Thứ", color="Số bài"),
                        x=matrix.columns,
                        y=matrix.index,
                        text_auto=True,    
                        aspect="auto",     
                        color_continuous_scale="RdBu_r"
                    )

                    fig_heat.update_xaxes(dtick=1)
                    fig_heat.update_layout(title="Số lượng bài viết cụ thể theo từng khung giờ")
                    
                    st.plotly_chart(fig_heat, use_container_width=True)

                except Exception as e:
                    st.warning(f"Chưa đủ dữ liệu để vẽ bảng chi tiết: {e}")
            else:
                st.warning("Dữ liệu thiếu thông tin thời gian.")

        with tab4:
            st.subheader("Đám mây từ khóa (Word Cloud)")
            if "text_clean" in df.columns:
                try:
                    text = " ".join(df["text_clean"].dropna().astype(str))
                    if len(text) > 100:
                        wc = WordCloud(width=800, height=400, background_color="white", 
                                       colormap="plasma", random_state=42).generate(text)
                        st.image(wc.to_array(), use_container_width=True)
                    else:
                        st.info("Không đủ dữ liệu văn bản để tạo Word Cloud.")
                except:
                    st.info("Lỗi tạo WordCloud.")
            
            st.divider()
            with st.expander("Xem dữ liệu thô (Bảng)", expanded=False):
                st.dataframe(df, use_container_width=True)

    else:
        st.warning("Không tìm thấy dữ liệu trong Collection này.")