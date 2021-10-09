from isqlite import (
    AutoTable,
    BooleanColumn,
    DecimalColumn,
    ForeignKeyColumn,
    IntegerColumn,
    TextColumn,
)

SCHEMA = [
    AutoTable(
        "departments",
        columns=[
            TextColumn("name", required=True),
            TextColumn("abbreviation", required=True),
        ],
    ),
    AutoTable(
        "professors",
        columns=[
            TextColumn("first_name", required=True),
            TextColumn("last_name", required=True),
            ForeignKeyColumn("department", model="departments", required=True),
            # DELETED:
            #   BooleanColumn("tenured", required=True),
            BooleanColumn("retired", required=True),
            ForeignKeyColumn("manager", model="professors"),
        ],
    ),
    AutoTable(
        "courses",
        columns=[
            IntegerColumn("course_number", required=True),
            ForeignKeyColumn("department", model="departments", required=True),
            ForeignKeyColumn("instructor", model="professors", required=True),
            TextColumn("title", required=True),
            DecimalColumn("credits", required=True),
        ],
    ),
    AutoTable(
        "students",
        columns=[
            IntegerColumn("student_id", required=True),
            TextColumn("first_name", required=True),
            TextColumn("last_name", required=True),
            ForeignKeyColumn("major", model="departments"),
            IntegerColumn("graduation_year", required=True),
            # ADDED:
            TextColumn("dormitory", required=False),
        ],
    ),
]
