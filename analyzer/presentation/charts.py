"""Модуль построения графиков через Plotly."""

import logging
from pathlib import Path
from typing import List

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("./data/charts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

_DAY_NAMES: List[str] = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def _save(fig: go.Figure, stem: str) -> Path:
    """Сохраняет фигуру в HTML и PNG (PNG требует kaleido).

    Args:
        fig: Plotly-фигура для сохранения.
        stem: Имя файла без расширения.

    Returns:
        Путь к HTML-файлу.
    """
    html_path = OUTPUT_DIR / f"{stem}.html"
    fig.write_html(str(html_path))
    logger.info("chart saved: %s", html_path)

    try:
        png_path = OUTPUT_DIR / f"{stem}.png"
        fig.write_image(str(png_path))
        logger.info("chart saved: %s", png_path)
    except Exception as exc:  # kaleido может отсутствовать
        logger.warning("PNG export skipped (%s): pip install kaleido", exc)

    return html_path


class ChartBuilder:
    """Строит и сохраняет информативные графики по данным конвейера."""

    def events_timeseries(self, df: pl.DataFrame) -> Path:
        """Строит временной ряд total_events по окнам (scatter + line).

        Args:
            df: DataFrame с колонками window_start, total_events.

        Returns:
            Путь к сохранённому HTML-файлу.
        """
        fig = px.line(
            df.to_pandas(),
            x="window_start",
            y="total_events",
            title="Количество событий по временным окнам",
            labels={"window_start": "Время", "total_events": "Событий"},
        )
        fig.update_traces(mode="lines+markers")
        fig.update_layout(
            xaxis_title="Время (UTC)",
            yaxis_title="Количество событий",
            hovermode="x unified",
        )
        return _save(fig, "events_timeseries")

    def event_type_distribution(self, df: pl.DataFrame) -> Path:
        """Строит круговую диаграмму распределения типов событий.

        Args:
            df: DataFrame с колонками event_type, total_count.

        Returns:
            Путь к сохранённому файлу.
        """
        fig = px.pie(
            df.to_pandas(),
            names="event_type",
            values="total_count",
            title="Распределение типов событий",
            hole=0.35,  # donut-стиль для читаемости
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>Событий: %{value}<br>Доля: %{percent}",
        )
        fig.update_layout(legend_title_text="Тип события")
        return _save(fig, "event_type_distribution")

    def unique_users_heatmap(self, df: pl.DataFrame) -> Path:
        """Строит тепловую карту unique_users по часу и дню недели.

        Args:
            df: DataFrame с колонками window_start, unique_users.

        Returns:
            Путь к сохранённому файлу.
        """
        # Извлекаем час и день недели (ISO: 1=Пн … 7=Вс → индекс 0-6).
        enriched = df.with_columns(
            [
                pl.col("window_start").dt.hour().alias("hour"),
                (pl.col("window_start").dt.weekday() - 1).alias("day_idx"),
            ]
        )
        grouped = (
            enriched.group_by(["hour", "day_idx"])
            .agg(pl.col("unique_users").sum())
            .sort(["day_idx", "hour"])
            .to_pandas()
        )

        # Строим pivot: строки = часы (0-23), столбцы = дни недели.
        pivot = (
            grouped.pivot(index="hour", columns="day_idx", values="unique_users")
            .reindex(index=range(24), columns=range(7), fill_value=0)
            .fillna(0)
        )

        fig = go.Figure(
            data=go.Heatmap(
                z=pivot.values,
                x=_DAY_NAMES,
                y=[f"{h:02d}:00" for h in range(24)],
                colorscale="Blues",
                hoverongaps=False,
                hovertemplate=(
                    "День: %{x}<br>Час: %{y}<br>"
                    "Пользователей: %{z}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="Активность пользователей: час × день недели",
            xaxis_title="День недели",
            yaxis_title="Час суток",
            yaxis={"autorange": "reversed"},
        )
        return _save(fig, "unique_users_heatmap")

    def duration_histogram(self, df: pl.DataFrame) -> Path:
        """Строит гистограмму распределения avg_duration_ms.

        Args:
            df: DataFrame с колонкой avg_duration_ms.

        Returns:
            Путь к сохранённому файлу.
        """
        fig = px.histogram(
            df.to_pandas(),
            x="avg_duration_ms",
            nbins=40,
            title="Распределение средней длительности событий",
            labels={"avg_duration_ms": "Средняя длительность (мс)"},
            color_discrete_sequence=["#636EFA"],
        )
        fig.update_layout(
            xaxis_title="Средняя длительность события, мс",
            yaxis_title="Количество окон",
            bargap=0.05,
        )
        fig.add_vline(
            x=df["avg_duration_ms"].mean(),
            line_dash="dash",
            line_color="red",
            annotation_text="Среднее",
            annotation_position="top right",
        )
        return _save(fig, "duration_histogram")

    def performance_comparison(
        self,
        polars_ms: float,
        duckdb_ms: float,
    ) -> Path:
        """Строит bar chart сравнения производительности Polars vs DuckDB.

        Args:
            polars_ms: Время выполнения Polars в мс.
            duckdb_ms: Время выполнения DuckDB в мс.

        Returns:
            Путь к сохранённому файлу.
        """
        fig = go.Figure(
            data=[
                go.Bar(
                    x=["Polars", "DuckDB"],
                    y=[polars_ms, duckdb_ms],
                    marker_color=["#EF553B", "#00CC96"],
                    text=[f"{polars_ms:.1f} мс", f"{duckdb_ms:.1f} мс"],
                    textposition="outside",
                    hovertemplate="%{x}: %{y:.2f} мс<extra></extra>",
                )
            ]
        )
        speedup = polars_ms / duckdb_ms if duckdb_ms > 0 else 0.0
        fig.update_layout(
            title=(
                f"Производительность: Polars vs DuckDB"
                f"<br><sub>DuckDB быстрее в {speedup:.1f}x</sub>"
            ),
            xaxis_title="Инструмент",
            yaxis_title="Время выполнения (мс)",
            showlegend=False,
            yaxis={"rangemode": "tozero"},
        )
        return _save(fig, "performance_comparison")
