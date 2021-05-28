#include "duckdb/main/query_result.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/arrow.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

QueryResult::QueryResult(QueryResultType type, StatementType statement_type)
    : type(type), statement_type(statement_type), success(true) {
}

QueryResult::QueryResult(QueryResultType type, StatementType statement_type, vector<LogicalType> types_p,
                         vector<string> names_p)
    : type(type), statement_type(statement_type), types(move(types_p)), names(move(names_p)), success(true) {
	D_ASSERT(types.size() == names.size());
}

QueryResult::QueryResult(QueryResultType type, string error) : type(type), success(false), error(move(error)) {
}

unique_ptr<DataChunk> QueryResult::Fetch() {
	auto chunk = FetchRaw();
	if (!chunk) {
		return nullptr;
	}
	chunk->Normalify();
	return chunk;
}

bool QueryResult::Equals(QueryResult &other) {
	// first compare the success state of the results
	if (success != other.success) {
		return false;
	}
	if (!success) {
		return error == other.error;
	}
	// compare names
	if (names != other.names) {
		return false;
	}
	// compare types
	if (types != other.types) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	while (true) {
		auto lchunk = Fetch();
		auto rchunk = other.Fetch();
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		if (lchunk->size() != rchunk->size()) {
			return false;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
			for (idx_t row = 0; row < rchunk->size(); row++) {
				auto lvalue = lchunk->GetValue(col, row);
				auto rvalue = rchunk->GetValue(col, row);
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
}

void QueryResult::Print() {
	Printer::Print(ToString());
}

string QueryResult::HeaderToString() {
	string result;
	for (auto &name : names) {
		result += name + "\t";
	}
	result += "\n";
	for (auto &type : types) {
		result += type.ToString() + "\t";
	}
	result += "\n";
	return result;
}

struct DuckDBArrowSchemaHolder {
	// unused in children
	vector<ArrowSchema> children = {};
	// unused in children
	vector<ArrowSchema *> children_ptrs = {};
};

static void ReleaseDuckDBArrowSchema(ArrowSchema *schema) {
	if (!schema || !schema->release) {
		return;
	}
	schema->release = nullptr;
	auto holder = static_cast<DuckDBArrowSchemaHolder *>(schema->private_data);
	delete holder;
}

void QueryResult::ToArrowSchema(ArrowSchema *out_schema) {
	D_ASSERT(out_schema);

	// Allocate as unique_ptr first to cleanup properly on error
	auto root_holder = make_unique<DuckDBArrowSchemaHolder>();

	// Allocate the children
	root_holder->children.resize(ColumnCount());
	root_holder->children_ptrs.resize(ColumnCount(), nullptr);
	for (size_t i = 0; i < ColumnCount(); ++i) {
		root_holder->children_ptrs[i] = &root_holder->children[i];
	}
	out_schema->children = root_holder->children_ptrs.data();
	out_schema->n_children = ColumnCount();

	// Store the schema
	out_schema->format = "+s"; // struct apparently
	out_schema->flags = 0;
	out_schema->metadata = nullptr;
	out_schema->name = "duckdb_query_result";
	out_schema->dictionary = nullptr;

	// Configure all child schemas
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		auto &child = root_holder->children[col_idx];

		// Child is cleaned up by parent
		child.private_data = nullptr;
		child.release = ReleaseDuckDBArrowSchema;

		// Store the child schema
		child.flags = ARROW_FLAG_NULLABLE;
		child.name = names[col_idx].c_str();
		child.n_children = 0;
		child.children = nullptr;
		child.flags = 0;
		child.metadata = nullptr;
		child.dictionary = nullptr;

		switch (types[col_idx].id()) {
		case LogicalTypeId::BOOLEAN:
			child.format = "b";
			break;
		case LogicalTypeId::TINYINT:
			child.format = "c";
			break;
		case LogicalTypeId::SMALLINT:
			child.format = "s";
			break;
		case LogicalTypeId::INTEGER:
			child.format = "i";
			break;
		case LogicalTypeId::BIGINT:
			child.format = "l";
			break;
		case LogicalTypeId::UTINYINT:
			child.format = "C";
			break;
		case LogicalTypeId::USMALLINT:
			child.format = "S";
			break;
		case LogicalTypeId::UINTEGER:
			child.format = "I";
			break;
		case LogicalTypeId::UBIGINT:
			child.format = "L";
			break;
		case LogicalTypeId::FLOAT:
			child.format = "f";
			break;
		case LogicalTypeId::HUGEINT:
			child.format = "d:38,0";
			break;
		case LogicalTypeId::DOUBLE:
			child.format = "g";
			break;
		case LogicalTypeId::VARCHAR:
			child.format = "u";
			break;
		case LogicalTypeId::DATE:
			child.format = "tdD";
			break;
		case LogicalTypeId::TIME:
			child.format = "ttm";
			break;
		case LogicalTypeId::TIMESTAMP:
			child.format = "tsu:";
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
			child.format = "tss:";
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			child.format = "tsn:";
			break;
		case LogicalTypeId::TIMESTAMP_MS:
			child.format = "tsm:";
			break;
		default:
			throw NotImplementedException("Unsupported Arrow type " + types[col_idx].ToString());
		}
	}

	// Release ownership to caller
	out_schema->private_data = root_holder.release();
	out_schema->release = ReleaseDuckDBArrowSchema;
}

} // namespace duckdb
