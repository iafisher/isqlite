from isqlite import Schema, Table, columns

SCHEMA = Schema(
    [
        Table(
            "departments",
            columns=[
                "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT",
                "name TEXT NOT NULL CHECK(name != '')",
                "abbreviation TEXT NOT NULL CHECK(name != '')",
            ],
        ),
        Table(
            "professors",
            columns=[
                columns.primary_key("id"),
                columns.text("first_name"),
                columns.text("last_name"),
                columns.foreign_key("department", foreign_table="departments"),
                columns.boolean("tenured"),
                columns.boolean("retired"),
                columns.foreign_key(
                    "manager", foreign_table="professors", required=False
                ),
            ],
        ),
        Table(
            "courses",
            columns=[
                columns.primary_key("id"),
                columns.integer("course_number"),
                columns.foreign_key("department", foreign_table="departments"),
                columns.foreign_key("instructor", foreign_table="professors"),
                columns.text("title"),
                columns.decimal("credits"),
            ],
        ),
        Table(
            "students",
            columns=[
                columns.primary_key("id"),
                columns.integer("student_id"),
                columns.text("first_name"),
                columns.text("last_name"),
                columns.foreign_key(
                    "major", foreign_table="departments", required=False
                ),
                columns.integer("graduation_year"),
            ],
        ),
    ]
)
