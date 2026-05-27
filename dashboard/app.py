"""Streamlit дашборд для мониторинга конвейера пользовательских событий."""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

st.set_page_config(
    page_title="Анализ пользовательских событий",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

PARQUET_DIR = Path(os.getenv("PARQUET_OUTPUT_DIR", "./data"))
REFRESH_INTERVAL = int(os.getenv("STREAMLIT_REFRESH_INTERVAL", "5"))

_EVENT_TYPES = ["click", "page_view", "purchase", "login", "logout", "search"]


def load_data() -> pl.DataFrame | None:
    """Загружает все Parquet-файлы из директории данных и объединяет их.

    Returns:
        Объединённый DataFrame, отсортированный по window_start,
        или None если файлов нет.
    """
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        return None
    try:
        frames = [pl.read_parquet(f) for f in files]
        # diagonal_relaxed заполняет отсутствующие колонки (например, event_counts
        # в файлах, записанных до добавления колонки) значением null вместо ошибки.
        return pl.concat(frames, how="diagonal_relaxed").sort("window_start")
    except Exception:
        return None


def _aggregate_pages(df: pl.DataFrame) -> pl.DataFrame:
    """Разбирает JSON-колонку top_pages и суммирует счётчики по страницам.

    Args:
        df: DataFrame с колонкой top_pages (JSON-строка).

    Returns:
        DataFrame с колонками page и count, отсортированный по убыванию count.
    """
    records: list[dict] = []
    for row in df.iter_rows(named=True):
        try:
            pages = json.loads(row["top_pages"]) if row["top_pages"] else []
        except (json.JSONDecodeError, KeyError):
            pages = []
        for p in pages:
            records.append({"page": p["page"], "count": int(p["count"])})

    if not records:
        return pl.DataFrame(
            {
                "page": pl.Series([], dtype=pl.Utf8),
                "count": pl.Series([], dtype=pl.Int64),
            }
        )

    return (
        pl.DataFrame(records)
        .group_by("page")
        .agg(pl.col("count").sum())
        .sort("count", descending=True)
    )


def render_metrics(df: pl.DataFrame) -> None:
    """Отрисовывает блок метрик (4 KPI-карточки).

    Метрики:
        - Всего событий: сумма total_events по всем окнам
        - Уникальных пользователей: сумма unique_users по окнам
        - Среднее время отклика (ms): среднее avg_duration_ms
        - Активных окон: количество строк DataFrame

    Args:
        df: Отфильтрованный DataFrame конвейера.
    """
    total_events = int(df["total_events"].sum())
    total_users = int(df["unique_users"].sum())
    avg_duration = df["avg_duration_ms"].mean() or 0.0
    active_windows = df.height

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего событий", f"{total_events:,}")
    c2.metric("Уникальных пользователей", f"{total_users:,}")
    c3.metric("Среднее время отклика, мс", f"{avg_duration:.1f}")
    c4.metric("Активных окон", active_windows)


def render_timeseries(df: pl.DataFrame) -> None:
    """Отрисовывает временной ряд событий по окнам.

    Args:
        df: DataFrame с колонками window_start и total_events.
    """
    fig = px.line(
        df.to_pandas(),
        x="window_start",
        y="total_events",
        title="Количество событий по временным окнам",
        labels={"window_start": "Время", "total_events": "Событий"},
    )
    fig.update_traces(mode="lines+markers", marker_size=4)
    fig.update_layout(
        xaxis_title="Время (UTC)",
        yaxis_title="Количество событий",
        hovermode="x unified",
        margin={"t": 40, "b": 30},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_event_distribution(df: pl.DataFrame) -> None:
    """Отрисовывает распределение топ страниц (pie chart).

    Данные берутся из JSON-колонки top_pages и агрегируются
    по всем окнам. Отображаются топ-10 страниц.

    Args:
        df: DataFrame с колонкой top_pages.
    """
    pages_df = _aggregate_pages(df).head(10)
    if pages_df.is_empty():
        st.info("Нет данных о страницах.")
        return

    fig = px.pie(
        pages_df.to_pandas(),
        names="page",
        values="count",
        title="Распределение по страницам (топ-10)",
        hole=0.35,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(
        textposition="inside",
        textinfo="percent+label",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Событий: %{value}<br>"
            "Доля: %{percent}<extra></extra>"
        ),
    )
    fig.update_layout(
        legend_title_text="Страница",
        margin={"t": 40, "b": 30},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_top_pages(df: pl.DataFrame) -> None:
    """Отрисовывает топ-10 страниц по количеству событий (bar chart).

    Args:
        df: DataFrame с колонкой top_pages.
    """
    pages_df = _aggregate_pages(df).head(10)
    if pages_df.is_empty():
        st.info("Нет данных о страницах.")
        return

    fig = px.bar(
        pages_df.to_pandas(),
        x="count",
        y="page",
        orientation="h",
        title="Топ-10 страниц по событиям",
        labels={"count": "Событий", "page": "Страница"},
        color="count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        yaxis={"autorange": "reversed"},
        showlegend=False,
        coloraxis_showscale=False,
        margin={"t": 40, "b": 30},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_event_type_distribution(df: pl.DataFrame, selected_types: list) -> None:
    """Отрисовывает распределение событий по типам с фильтром выбранных типов.

    Парсит колонку event_counts (JSON) из каждого окна, суммирует по типам
    и отображает горизонтальный bar chart только для выбранных типов.

    Args:
        df: DataFrame с колонкой event_counts (JSON-строка).
        selected_types: Список типов событий для отображения.
    """
    totals: dict = {}
    for row in df.iter_rows(named=True):
        raw = row.get("event_counts") or "{}"
        try:
            counts = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            counts = {}
        for event_type, cnt in counts.items():
            totals[event_type] = totals.get(event_type, 0) + int(cnt)

    if not totals:
        st.info("Нет данных о типах событий (поле event_counts отсутствует в Parquet).")
        return

    filtered = {k: v for k, v in totals.items() if k in selected_types}
    if not filtered:
        st.info("Выбранные типы событий не найдены в данных.")
        return

    sorted_items = sorted(filtered.items(), key=lambda x: x[1])
    event_names = [k for k, _ in sorted_items]
    event_counts_vals = [v for _, v in sorted_items]

    fig = px.bar(
        x=event_counts_vals,
        y=event_names,
        orientation="h",
        title=f"Распределение событий по типам (фильтр: {len(selected_types)} из {len(_EVENT_TYPES)})",
        labels={"x": "Количество событий", "y": "Тип события"},
        color=event_counts_vals,
        color_continuous_scale="Viridis",
    )
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        margin={"t": 40, "b": 30},
    )
    st.plotly_chart(fig, use_container_width=True)


def render_performance_table(df: pl.DataFrame) -> None:
    """Отрисовывает таблицу производительности окон (sortable).

    Столбцы: Начало окна, Конец окна, Событий, Уник. пользователей,
    Avg длительность (мс). Сортировка по убыванию времени начала.

    Args:
        df: DataFrame конвейера.
    """
    display = (
        df.select(
            [
                pl.col("window_start").alias("Начало окна"),
                pl.col("window_end").alias("Конец окна"),
                pl.col("total_events").alias("Событий"),
                pl.col("unique_users").alias("Уник. пользователей"),
                pl.col("avg_duration_ms").round(2).alias("Avg длительность, мс"),
            ]
        )
        .sort("Начало окна", descending=True)
        .to_pandas()
    )
    st.dataframe(display, use_container_width=True, hide_index=True)


def main() -> None:
    """Основная функция дашборда с авторефрешем.

    Загружает данные из Parquet, применяет фильтры боковой панели
    и отрисовывает все блоки. Перезапускается каждые REFRESH_INTERVAL
    секунд через st.rerun().
    """
    st.title("📊 Анализ пользовательских событий")
    st.caption(f"Обновление каждые {REFRESH_INTERVAL} секунд")

    with st.sidebar:
        st.header("Настройки")

        selected_event_types = st.multiselect(
            "Тип события",
            options=_EVENT_TYPES,
            default=_EVENT_TYPES,
            help="Фильтр по типу события — влияет на график распределения событий",
        )

        today = datetime.now(tz=timezone.utc).date()
        date_range = st.date_input(
            "Временной диапазон",
            value=(today - timedelta(days=1), today),
            help="Фильтр по дате начала временного окна",
        )

        st.divider()
        if st.button("🔄 Обновить сейчас", use_container_width=True):
            st.rerun()

        st.caption(f"Источник: {PARQUET_DIR}")

    df = load_data()

    if df is None:
        st.warning("Данные ещё не поступали. Ожидание...")
        time.sleep(REFRESH_INTERVAL)
        st.rerun()
        return

    # Применяем фильтр временного диапазона.
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_dt = datetime.combine(
            date_range[0], datetime.min.time()
        ).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(
            date_range[1], datetime.max.time()
        ).replace(tzinfo=timezone.utc)
        df = df.filter(
            (pl.col("window_start") >= start_dt)
            & (pl.col("window_start") <= end_dt)
        )

    if df.is_empty():
        st.warning("Нет данных в выбранном временном диапазоне.")
        time.sleep(REFRESH_INTERVAL)
        st.rerun()
        return

    render_metrics(df)

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        render_timeseries(df)
    with col2:
        render_event_distribution(df)

    st.divider()
    render_event_type_distribution(df, selected_event_types)

    render_top_pages(df)

    st.subheader("Таблица производительности окон")
    render_performance_table(df)

    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
