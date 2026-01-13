# app.py
import streamlit as st
import data_loader as dl 
import visualizer as vis 

st.set_page_config(page_title="BigData Analytics", layout="wide")
st.title("BigData Analytics: Wikipedia & Spark")

if "data" not in st.session_state:
    st.session_state.data = None

st.sidebar.header("Control Panel")

if st.sidebar.button("Kích hoạt xử lý Spark"):
    with st.spinner("Spark đang xử lý phân tán..."):
        st.session_state.data = dl.load_and_process_data("wiki", "pages_clean")
        
        if not st.session_state.data:
            st.error("Không tải được dữ liệu.")

if st.session_state.data is not None:
    data = st.session_state.data
    
    df_raw = data.get("raw_data") 

    st.success("Dữ liệu đã sẵn sàng!")

    kpi = data["kpi"]
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng số bài viết", f"{kpi['total_docs']:,}")
    c2.metric("Dữ liệu mới nhất", int(kpi['latest_year']))
    c3.metric("Bài dài nhất (từ)", f"{kpi['max_len']:,}")

    tab1, tab2 = st.tabs(["Dashboard Tổng quan", "Phân tích Nội dung"])
    
    with tab1: 
        col1, col2 = st.columns(2)
        with col1:
            fig_year = vis.plot_articles_over_time(data["stats_year"])
            st.plotly_chart(fig_year, use_container_width=True)
        with col2:
            fig_top = vis.plot_top_10_longest(data["top_10"])
            st.plotly_chart(fig_top, use_container_width=True)
        
        st.divider()
        
        col3, col4 = st.columns([2, 1])
        with col3:
            show_full = st.checkbox("Hiển thị cả dữ liệu ngoại lai (Outliers)")
            fig_hist = vis.plot_word_count_distribution(data["distribution"], show_full)
            if fig_hist:
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.warning("Không có dữ liệu phân bố.")
        with col4:
            st.info("Biểu đồ phân bố giúp phát hiện các bài viết có độ dài bất thường (quá ngắn hoặc quá dài) so với trung bình.")

    with tab2:
        st.subheader("Đám mây từ khóa (Word Cloud)")
        wc_image = vis.generate_wordcloud(data["sample_text"])
        if wc_image is not None:
            col_wc_1, col_wc_2, col_wc_3 = st.columns([1, 6, 1])
            with col_wc_2:
                st.image(wc_image, use_container_width=True)
        else:
            st.warning("Không đủ dữ liệu text để tạo WordCloud.")

    st.divider()

    with st.expander("Xem dữ liệu chi tiết & Tìm kiếm", expanded=False):
        if df_raw is not None:
            col_search_1, col_search_2 = st.columns([2, 1])
            
            with col_search_1:
                search_term = st.text_input("Tìm kiếm (Tiêu đề hoặc Nội dung):", placeholder="Nhập từ khóa...")

            with col_search_2:
                all_cols = df_raw.columns.tolist()
                default_cols = [c for c in ['title', 'year', 'word_count'] if c in all_cols]
                selected_cols = st.multiselect("Chọn cột hiển thị:", all_cols, default=default_cols)

            df_display = df_raw.copy()
            
            if search_term:
                mask = df_display['title'].astype(str).str.contains(search_term, case=False, na=False)
                if 'text_clean' in df_display.columns:
                    mask |= df_display['text_clean'].astype(str).str.contains(search_term, case=False, na=False)
                
                df_display = df_display[mask]
                st.caption(f"Tìm thấy **{len(df_display)}** kết quả.")

            if not selected_cols:
                st.warning("Vui lòng chọn ít nhất một cột.")
            else:
                st.dataframe(
                    df_display[selected_cols], 
                    use_container_width=True, 
                    hide_index=True,
                    height=400
                )
        else:
            st.error("Không tìm thấy dữ liệu gốc (raw_data) để hiển thị bảng.")