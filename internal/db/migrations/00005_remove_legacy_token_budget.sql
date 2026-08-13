-- +goose Up
update scheduler_settings
set settings = settings #- '{topicEvaluate,dailyTokenBudget}',
    updated_at = now()
where settings #> '{topicEvaluate,dailyTokenBudget}' is not null;

-- +goose Down
