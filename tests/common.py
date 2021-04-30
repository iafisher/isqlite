import decimal

from isqlite import Schema, Table, columns


def get_schema():
    return Schema(
        [
            Table(
                "departments",
                [
                    columns.Text("name", required=True),
                    columns.Text("abbreviation", required=True),
                ],
            ),
            Table(
                "professors",
                [
                    columns.Text("first_name", required=True),
                    columns.Text("last_name", required=True),
                    columns.ForeignKey("department", "departments", required=True),
                    columns.Boolean("tenured", required=True),
                    columns.Boolean("retired", required=True),
                ],
            ),
            Table(
                "courses",
                [
                    columns.Integer("course_number", required=True),
                    columns.ForeignKey("department", "departments", required=True),
                    columns.ForeignKey("instructor", "professors", required=True),
                    columns.Text("title", required=True),
                    columns.Decimal("credits", required=True),
                ],
            ),
            Table(
                "students",
                [
                    columns.Integer("student_id", required=True),
                    columns.Text("first_name", required=True),
                    columns.Text("last_name", required=True),
                    columns.ForeignKey("major", "departments", required=False),
                    columns.Integer("graduation_year", required=True),
                ],
            ),
        ]
    )


def create_test_data(db):
    cs_department = db.create(
        "departments", {"name": "Computer Science", "abbreviation": "CS"}
    )
    ling_department = db.create(
        "departments", {"name": "Linguistics", "abbreviation": "LING"}
    )
    donald_knuth = db.create(
        "professors",
        {
            "first_name": "Donald",
            "last_name": "Knuth",
            "department": cs_department,
            "tenured": True,
            "retired": False,
        },
    )
    noam_chomsky = db.create(
        "professors",
        {
            "first_name": "Noam",
            "last_name": "Chomsky",
            "department": ling_department,
            "tenured": True,
            "retired": True,
        },
    )
    db.create_many(
        "courses",
        [
            {
                "course_number": 399,
                "department": cs_department,
                "instructor": donald_knuth,
                "title": "Algorithms",
                "credits": decimal.Decimal(2.0),
            },
            {
                "couse_number": 101,
                "department": ling_department,
                "instructor": noam_chomsky,
                "title": "Intro to Linguistics",
                "credits": decimal.Decimal(1.0),
            },
        ],
    )
    db.create_many(
        "students",
        [
            {
                "student_id": 123456,
                "first_name": "Helga",
                "last_name": "Heapsort",
                "major": cs_department,
                "graduation_year": 2023,
            },
            {
                "student_id": 456789,
                "first_name": "Philip",
                "last_name": "Phonologist",
                "major": ling_department,
                "graduation_year": 2022,
            },
        ],
    )
    db.commit()
