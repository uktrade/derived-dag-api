from marshmallow import Schema, fields


class ConfigSchema(Schema):
    sql = fields.Str(required=False)


class DerivedDagInputSchema(Schema):
    """
    Parameters:
     - type (str)
     - schedule (str)
     - schema_name (str)
     - table_name (str)
     - config (json)
     - enabled (bool)
    """
    type = fields.Str(required=True)
    schedule = fields.Str(required=True)
    schema_name = fields.Str(required=True)
    table_name = fields.Str(required=True)
    config = fields.Nested(ConfigSchema, required=True)
    enabled = fields.Bool(required=False, default=True)
