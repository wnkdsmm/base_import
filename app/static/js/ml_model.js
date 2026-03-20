(function () {
    function byId(id) {
        return document.getElementById(id);
    }

    function escapeHtml(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/\"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function renderFallback(chartNode, fallbackNode, message) {
        if (!chartNode || !fallbackNode) {
            return;
        }
        chartNode.innerHTML = '';
        fallbackNode.textContent = message || 'Нет данных для графика.';
        fallbackNode.classList.remove('is-hidden');
    }

    function renderLineChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var series = chart && chart.series;
        if (!series || !Array.isArray(series.history) || !series.history.length) {
            renderFallback(chartNode, fallbackNode, chart && chart.empty_message);
            return;
        }

        fallbackNode.classList.add('is-hidden');
        var history = series.history || [];
        var backtestActual = series.backtest_actual || [];
        var backtestPredicted = series.backtest_predicted || [];
        var forecast = series.forecast || [];
        var band = series.forecast_band || [];
        var orderedDates = [];
        var seen = {};

        [history, backtestActual, backtestPredicted, forecast].forEach(function (points) {
            points.forEach(function (point) {
                if (!seen[point.x]) {
                    seen[point.x] = true;
                    orderedDates.push(point.x);
                }
            });
        });

        if (!orderedDates.length) {
            renderFallback(chartNode, fallbackNode, chart && chart.empty_message);
            return;
        }

        var xIndex = {};
        orderedDates.forEach(function (date, index) {
            xIndex[date] = index;
        });

        var values = [];
        history.forEach(function (point) { values.push(point.y); });
        backtestActual.forEach(function (point) { values.push(point.y); });
        backtestPredicted.forEach(function (point) { values.push(point.y); });
        forecast.forEach(function (point) { values.push(point.y); });
        band.forEach(function (point) {
            values.push(point.low);
            values.push(point.high);
        });

        var isPercent = chart && chart.value_format === 'percent';
        var yMin = 0;
        var yMax = Math.max.apply(null, values.concat([1]));
        if (isPercent) {
            yMax = Math.min(100, Math.max(25, Math.ceil((yMax + 5) / 5) * 5));
        } else {
            yMax = Math.max(1, Math.ceil((yMax + 0.5) * 2) / 2);
        }
        if (yMax <= yMin) {
            yMax = yMin + 1;
        }

        var width = 920;
        var height = chartId === 'mlImportanceChart' ? 320 : 420;
        var padding = { top: 20, right: 24, bottom: 54, left: 54 };
        var innerWidth = width - padding.left - padding.right;
        var innerHeight = height - padding.top - padding.bottom;
        var denominator = Math.max(orderedDates.length - 1, 1);

        function x(date) {
            return padding.left + (xIndex[date] / denominator) * innerWidth;
        }

        function y(value) {
            return padding.top + innerHeight - ((value - yMin) / (yMax - yMin)) * innerHeight;
        }

        function linePath(points) {
            var path = '';
            points.forEach(function (point, index) {
                var px = x(point.x);
                var py = y(point.y);
                path += (index ? ' L ' : 'M ') + px.toFixed(2) + ' ' + py.toFixed(2);
            });
            return path;
        }

        var axisLines = '';
        var gridLines = '';
        for (var step = 0; step <= 4; step += 1) {
            var value = yMin + ((yMax - yMin) * step / 4);
            var py = y(value);
            gridLines += '<line x1="' + padding.left + '" y1="' + py.toFixed(2) + '" x2="' + (width - padding.right) + '" y2="' + py.toFixed(2) + '" class="ml-grid-line"></line>';
            var axisValue = Math.round(value * 10) / 10;
            var axisText = isPercent
                ? String(axisValue).replace('.', ',') + '%'
                : String(axisValue).replace('.', ',');
            axisLines += '<text x="' + (padding.left - 10) + '" y="' + (py + 4).toFixed(2) + '" text-anchor="end" class="ml-axis-label">' + escapeHtml(axisText) + '</text>';
        }

        var tickIndexes = [0, Math.floor((orderedDates.length - 1) / 2), orderedDates.length - 1]
            .filter(function (value, index, arr) { return arr.indexOf(value) === index && value >= 0; });
        tickIndexes.forEach(function (index) {
            var tickDate = orderedDates[index];
            axisLines += '<text x="' + x(tickDate).toFixed(2) + '" y="' + (height - 16) + '" text-anchor="middle" class="ml-axis-label">' + escapeHtml(formatDate(tickDate)) + '</text>';
        });

        var bandPath = '';
        if (band.length) {
            var upper = band.map(function (point) { return x(point.x).toFixed(2) + ' ' + y(point.high).toFixed(2); }).join(' L ');
            var lower = band.slice().reverse().map(function (point) { return x(point.x).toFixed(2) + ' ' + y(point.low).toFixed(2); }).join(' L ');
            bandPath = '<path d="M ' + upper + ' L ' + lower + ' Z" class="ml-band-path"></path>';
        }

        var svg = ''
            + '<svg viewBox="0 0 ' + width + ' ' + height + '" class="ml-svg-chart" preserveAspectRatio="none">'
            + gridLines
            + '<line x1="' + padding.left + '" y1="' + (height - padding.bottom) + '" x2="' + (width - padding.right) + '" y2="' + (height - padding.bottom) + '" class="ml-axis-line"></line>'
            + '<line x1="' + padding.left + '" y1="' + padding.top + '" x2="' + padding.left + '" y2="' + (height - padding.bottom) + '" class="ml-axis-line"></line>'
            + bandPath;

        if (history.length) {
            svg += '<path d="' + linePath(history) + '" class="ml-line-history"></path>';
        }
        if (backtestActual.length) {
            svg += '<path d="' + linePath(backtestActual) + '" class="ml-line-backtest-actual"></path>';
        }
        if (backtestPredicted.length) {
            svg += '<path d="' + linePath(backtestPredicted) + '" class="ml-line-backtest"></path>';
        }
        if (forecast.length) {
            svg += '<path d="' + linePath(forecast) + '" class="ml-line-forecast"></path>';
        }
        forecast.forEach(function (point) {
            svg += '<circle cx="' + x(point.x).toFixed(2) + '" cy="' + y(point.y).toFixed(2) + '" r="3.5" class="ml-forecast-point"></circle>';
        });
        svg += axisLines + '</svg>';

        var legend = '';
        if (Array.isArray(chart.legend) && chart.legend.length) {
            legend = '<div class="ml-chart-legend">' + chart.legend.map(function (item) {
                return '<span class="ml-chart-legend-item"><i style="--legend-color:' + escapeHtml(item.color) + '"></i>' + escapeHtml(item.label) + '</span>';
            }).join('') + '</div>';
        }

        chartNode.innerHTML = legend + '<div class="ml-chart-shell">' + svg + '</div>';
    }

    function renderBarsChart(chart, chartId, fallbackId) {
        var chartNode = byId(chartId);
        var fallbackNode = byId(fallbackId);
        if (!chartNode || !fallbackNode) {
            return;
        }

        var items = chart && chart.items;
        if (!Array.isArray(items) || !items.length) {
            renderFallback(chartNode, fallbackNode, chart && chart.empty_message);
            return;
        }

        fallbackNode.classList.add('is-hidden');
        var maxValue = Math.max.apply(null, items.map(function (item) { return item.value; }).concat([1]));
        var html = '<div class="ml-bars">';
        items.forEach(function (item) {
            var percent = Math.max(8, Math.round((item.value / maxValue) * 100));
            html += ''
                + '<div class="ml-bar-row">'
                + '<div class="ml-bar-meta"><span>' + escapeHtml(item.label) + '</span><strong>' + escapeHtml(item.value_display) + '%</strong></div>'
                + '<div class="ml-bar-track"><div class="ml-bar-fill" style="--ml-bar-width:' + percent + '%"></div></div>'
                + '</div>';
        });
        html += '</div>';
        chartNode.innerHTML = html;
    }

    function formatDate(value) {
        if (!value || value.length < 10) {
            return value || '';
        }
        return value.slice(8, 10) + '.' + value.slice(5, 7);
    }

    document.addEventListener('DOMContentLoaded', function () {
        var data = window.__FIRE_ML_INITIAL__ || {};
        var charts = data.charts || {};
        renderLineChart(charts.forecast, 'mlForecastChart', 'mlForecastChartFallback');
        renderBarsChart(charts.importance, 'mlImportanceChart', 'mlImportanceChartFallback');
    });
})();
