from isqlite import (
    BooleanColumn,
    DecimalColumn,
    ForeignKeyColumn,
    IntegerColumn,
    PrimaryKeyColumn,
    Table,
    TextColumn,
)

SCHEMA = [
    Table(
        "departments",
        columns=[
            PrimaryKeyColumn("id"),
            TextColumn("name", required=True),
            TextColumn("abbreviation", required=True),
        ],
    ),
    Table(
        "professors",
        columns=[
            PrimaryKeyColumn("id"),
            TextColumn("first_name", required=True),
            TextColumn("last_name", required=True),
            ForeignKeyColumn("department", model="departments", required=True),
            # DELETED:
            #   BooleanColumn("tenured", required=True),
            BooleanColumn("retired", required=True),
            ForeignKeyColumn("manager", model="professors"),
        ],
    ),
    Table(
        "courses",
        columns=[
            PrimaryKeyColumn("id"),
            IntegerColumn("course_number", required=True),
            ForeignKeyColumn("department", model="departments", required=True),
            ForeignKeyColumn("instructor", model="professors", required=True),
            TextColumn("title", required=True),
            DecimalColumn("credits", required=True),
        ],
    ),
    Table(
        "students",
        columns=[
            PrimaryKeyColumn("id"),
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
