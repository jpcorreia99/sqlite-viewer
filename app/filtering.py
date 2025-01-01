import operator


class ValueFilter:
    column: str
    operator: any  # some operation exported by the operator module
    threshold: any

    def __init__(self, column: str, operator: str, threshold: any):
        self.column = column
        self.operator = ValueFilter._string_to_operator(operator)
        self.threshold = threshold

    def __call__(self, row) -> bool:
        if self.column not in row:
            raise ValueError(
                f"Expected column '{self.column}' in row but only found {row.keys}"
            )

        value = row[self.column]
        if not value:
            return False

        value = value.decode("utf8")
        if type(value) is not type(self.threshold):
            raise TypeError(
                "Type of given value is not the same as threshold: ",
                type(value),
                type(self.threshold),
            )

        return self.operator(value.strip().lower(), self.threshold.strip().lower())

    @staticmethod
    def _string_to_operator(operator_str: str):
        match operator_str:
            case "=":
                return operator.eq
            case _:
                raise TypeError(f"Operator '{operator_str}' is not yet supported")
