// Shared helpers and state

    var currentMlData = null;
    var currentJobState = null;
    var jobPollTimer = null;
    var isFetching = false;
    var progressTimers = [];
    var progressSteps = [
        {
            label: 'Загрузка данных',
            lead: 'Загружаем данные ML-прогноза',
            message: 'Получаем выбранный срез и обновляем параметры страницы.'
        },
        {
            label: 'Агрегация',
            lead: 'Агрегируем историю',
            message: 'Собираем дневной ряд, фильтры и доступные признаки.'
        },
        {
            label: 'Обучение / валидация',
            lead: 'Обучение и валидация',
            message: 'Считаем backtesting, прогноз и итоговые таблицы.'
        },
        {
            label: 'Построение визуализаций',
            lead: 'Обновляем визуализации',
            message: 'Подставляем графики, таблицы и карточки результата.'
        }
    ];

    function byId(id) {
        return document.getElementById(id);
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function setText(id, value) {
        var node = byId(id);
        if (node) {
            node.textContent = value == null ? '' : value;
        }
    }

    function setHref(id, href) {
        var node = byId(id);
        if (node && href) {
            node.setAttribute('href', href);
        }
    }

    function setValue(id, value) {
        var node = byId(id);
        if (node) {
            node.value = value == null ? '' : value;
        }
    }

    function setSelectOptions(id, options, selectedValue, emptyLabel) {
        var selectNode = byId(id);
        if (!selectNode) {
            return;
        }

        var safeOptions = Array.isArray(options) && options.length ? options : [{ value: 'all', label: emptyLabel }];
        selectNode.innerHTML = safeOptions.map(function (option) {
            var selected = String(option.value) === String(selectedValue) ? ' selected' : '';
            return '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
        }).join('');
    }

    function normalizePercent(value, fallback) {
        var normalizedFallback = fallback || '0%';
        var rawValue = String(value == null ? '' : value).trim();
        var match = rawValue.match(/^(-?\d+(?:\.\d+)?)%?$/);
        if (!match) {
            return normalizedFallback;
        }

        var numericValue = Math.max(0, Math.min(100, Number(match[1])));
        return numericValue + '%';
    }

    function normalizeCssColor(value, fallback) {
        var normalizedFallback = fallback || 'currentColor';
        var candidate = String(value == null ? '' : value).trim();
        if (!candidate) {
            return normalizedFallback;
        }

        var probe = document.createElement('span');
        probe.style.color = '';
        probe.style.color = candidate;
        return probe.style.color ? candidate : normalizedFallback;
    }

    function applyChartDecorators(root) {
        var scope = root && typeof root.querySelectorAll === 'function' ? root : document;
        Array.prototype.forEach.call(scope.querySelectorAll('[data-legend-color]'), function (node) {
            node.style.setProperty('--legend-color', normalizeCssColor(node.getAttribute('data-legend-color'), 'currentColor'));
        });
        Array.prototype.forEach.call(scope.querySelectorAll('[data-bar-width]'), function (node) {
            node.style.setProperty('--ml-bar-width', normalizePercent(node.getAttribute('data-bar-width'), '0%'));
        });
    }
