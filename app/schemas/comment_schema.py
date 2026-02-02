from app.extensions.extensions import ma



class CommentResponseSchema(ma.Schema):
    id = ma.Int()
    text = ma.Str()
    score = ma.Int()
    created_at = ma.DateTime()
    replies = ma.List(ma.Nested(lambda: CommentResponseSchema()))
