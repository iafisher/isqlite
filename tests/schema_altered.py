from isqlite import Table, columns

SCHEMA = [
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
            columns.text("first_name", required=True),
            columns.text("last_name", required=True),
            columns.foreign_key(
                "department", foreign_table="departments", required=True
            ),
            # DELETED:
            #   columns.boolean("tenured", required=True),
            columns.boolean("retired", required=True),
            columns.foreign_key("manager", foreign_table="professors"),
        ],
    ),
    Table(
        "courses",
        columns=[
            columns.primary_key("id"),
            columns.integer("course_number", required=True),
            columns.foreign_key(
                "department", foreign_table="departments", required=True
            ),
            columns.foreign_key(
                "instructor", foreign_table="professors", required=True
            ),
            columns.text("title", required=True),
            columns.decimal("credits", required=True),
        ],
    ),
    Table(
        "students",
        columns=[
            columns.primary_key("id"),
            columns.integer("student_id", required=True),
            columns.text("first_name", required=True),
            columns.text("last_name", required=True),
            columns.foreign_key("major", foreign_table="departments"),
            columns.integer("graduation_year", required=True),
            # ADDED:
            columns.text("dormitory", required=False),
        ],
    ),
]
