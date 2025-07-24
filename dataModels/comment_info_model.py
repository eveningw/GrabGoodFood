class comment_info:
    def __init__(self):
        self.commenter_name = ""
        self.commenter_detail = None
        self.comment_star_point = 0
        self.comment_date_record = "" #幾天前
        self.comment_date = None
        self.comment_content = ""
        self.created_at = None
        self.modified_at = None

    def to_dict(self):
        return{
            "commenter_name": self.commenter_name,
            "commenter_detail": self.commenter_detail,
            "comment_star_point": self.comment_star_point,
            "comment_date_record": self.comment_date_record,
            "comment_date": self.comment_date,
            "comment_content": self.comment_content
        }
       