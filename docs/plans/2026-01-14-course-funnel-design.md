# Course Funnel Analytics Design

## Overview

Analyze course completion funnels by weekly cohorts. Track how many users start and complete each lesson, and ultimately finish the course.

## Requirements

1. Per-course completion rates by weekly cohort
2. Per-lesson started/completed counts
3. Drop-off analysis between lessons

## Data Model

### Table 1: `mart_course_funnel_cohorts`

**Grain:** One row per course + cohort week

**Primary Key:** `course_id` + `cohort_week_start`

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | INTEGER | Course identifier |
| `cohort_week_start` | DATE | Monday of week users first started |
| `users_started` | INTEGER | Users who started any lesson |
| `users_completed_course` | INTEGER | Users who completed entire course |
| `course_completion_rate` | NUMERIC | users_completed_course / users_started |
| `total_lessons` | INTEGER | Number of lessons in this course |

### Table 2: `mart_course_funnel_lessons`

**Grain:** One row per course + cohort week + lesson

**Primary Key:** `course_id` + `cohort_week_start` + `lesson_id`

| Column | Type | Description |
|--------|------|-------------|
| `course_id` | INTEGER | Course identifier |
| `cohort_week_start` | DATE | Monday of week users first started |
| `lesson_id` | INTEGER | Lesson number |
| `users_started` | INTEGER | Users who started this lesson |
| `users_completed` | INTEGER | Users who completed this lesson |
| `lesson_completion_rate` | NUMERIC | users_completed / users_started |
| `funnel_retention_rate` | NUMERIC | users_started / cohort_users_started |
| `avg_time_spent_sec` | NUMERIC | Average time on this lesson |

## Source Events (Amplitude)

| Event | Properties | Usage |
|-------|------------|-------|
| `course_lesson_started` | course_id, lesson_id, entry_point | Lesson start |
| `course_lesson_completed` | course_id, lesson_id, time_spent_sec, course_progress | Lesson completion |
| `course_completed` | course_id | Course completion |

## Data Flow

1. Filter `raw_amplitude.events` for course events
2. Parse `event_properties` JSON to extract course_id, lesson_id, time_spent_sec
3. Determine cohort per user+course: week of first `course_lesson_started`
4. Aggregate to cohort level (Table 1) and lesson level (Table 2)

## Business Rules

1. **User starts lesson multiple times** — Count as 1 user (DISTINCT)
2. **User completes without start event** — Still count as completed
3. **Course with no completions** — Show 0 for completed, NULL for rate
4. **Lesson ordering** — By lesson_id as-is (1, 2, 3...)
5. **Time spent** — From `course_lesson_completed` events only
6. **Total lessons** — MAX(lesson_id) with completions in cohort

## Key Relationships

```
raw_amplitude.events.user_id ──> raw_funnelfox.sessions.profile_id
```
