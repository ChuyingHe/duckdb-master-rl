#include "duckdb/parser/expression/table_star_expression.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

TableStarExpression::TableStarExpression(string relation_name)
    : ParsedExpression(ExpressionType::TABLE_STAR, ExpressionClass::TABLE_STAR), relation_name(move(relation_name)) {
}

string TableStarExpression::ToString() const {
	return relation_name + ".*";
}

bool TableStarExpression::Equals(const TableStarExpression *a, const TableStarExpression *b) {
	return a->relation_name == b->relation_name;
}

unique_ptr<ParsedExpression> TableStarExpression::Copy() const {
	auto copy = make_unique<TableStarExpression>(relation_name);
	copy->CopyProperties(*this);
	return move(copy);
}

void TableStarExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(relation_name);
}

unique_ptr<ParsedExpression> TableStarExpression::Deserialize(ExpressionType type, Deserializer &source) {
	return make_unique<TableStarExpression>(source.Read<string>());
}

} // namespace duckdb
