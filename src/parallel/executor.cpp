#include "duckdb/execution/executor.hpp"

#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <algorithm>

namespace duckdb {

Executor::Executor(ClientContext &context) : context(context) {
}

Executor::~Executor() {
}

void Executor::InitializeForRL(PhysicalOperator *plan, int simulation_count) {
    //printf("Executor::InitializeForRL \n");
    Reset();

    physical_plan = plan;
    physical_state = physical_plan->GetOperatorState();

    context.profiler.Initialize(physical_plan);
    auto &scheduler = TaskScheduler::GetScheduler(context);
    this->producer = scheduler.CreateProducer();        // returns unique_ptr<ProducerToken>
    BuildPipelines(physical_plan, nullptr);     //create this->pipelines
    this->total_pipelines = pipelines.size();

    // schedule pipelines that do not have dependents
    for (auto &pipeline : pipelines) {
        if (!pipeline->HasDependencies()) {
            pipeline->Schedule();   // if current pipeline doesn't have dependencies/child, then pipeline.total_tasks+1
        }
    }
    //printf("Executor::InitializeForRL - 8; ");
    // now execute tasks from this producer until all pipelines are completed
    while (completed_pipelines < total_pipelines) {
        //printf("Executor::InitializeForRL - 9; ");           // 1 time
        unique_ptr<Task> task;
        while (scheduler.GetTaskFromProducer(*producer, task)) {
            //printf("\n 🐱 Executor::InitializeForRL - a pipeline: \n");     // 4 times
            task->Execute();    // get tasks from pipelines  = pipeline which doesn't have dependencies
            task.reset();
        }
    }
    //printf("Executor::InitializeForRL - 11 after execute over task of pipelines \n");

    pipelines.clear();
    if (!exceptions.empty()) {
        // an exception has occurred executing one of the pipelines
        throw Exception(exceptions[0]);
    }
}


void Executor::Initialize(PhysicalOperator *plan) {
    //printf("Executor::Initialize \n");
	Reset();

	physical_plan = plan;
	physical_state = physical_plan->GetOperatorState();

	context.profiler.Initialize(physical_plan);
	auto &scheduler = TaskScheduler::GetScheduler(context);
	this->producer = scheduler.CreateProducer();        // returns unique_ptr<ProducerToken>
	BuildPipelines(physical_plan, nullptr);     //create this->pipelines
	this->total_pipelines = pipelines.size();

	// schedule pipelines that do not have dependents
	for (auto &pipeline : pipelines) {
		if (!pipeline->HasDependencies()) {
			pipeline->Schedule();   // if current pipeline doesn't have dependencies/child, then pipeline.total_tasks+1
		}
	}
    //printf("Executor::Initialize - 8; ");
	// now execute tasks from this producer until all pipelines are completed
	while (completed_pipelines < total_pipelines) {
	    //printf("Executor::Initialize - 9; ");           // 1 time
		unique_ptr<Task> task;
		while (scheduler.GetTaskFromProducer(*producer, task)) {
            //printf("\n 🐱 Executor::InitializeForRL - 10 \n");     // 4 times
			task->Execute();    // get tasks from pipelines  = pipeline which doesn't have dependencies
			task.reset();
		}
	}
    //printf("Executor::Initialize - 11 after execute over task of pipelines \n");

	pipelines.clear();
	if (!exceptions.empty()) {
		// an exception has occurred executing one of the pipelines
		throw Exception(exceptions[0]);
	}
}

void Executor::Reset() {
	delim_join_dependencies.clear();
	recursive_cte = nullptr;
	physical_plan = nullptr;
	physical_state = nullptr;
	completed_pipelines = 0;
	total_pipelines = 0;
	exceptions.clear();
	pipelines.clear();
}
/*	BuildPipelines(physical_plan, nullptr); //update this->pipelines */
void Executor::BuildPipelines(PhysicalOperator *op, Pipeline *parent) {
	if (op->IsSink()) {
		// operator is a sink, build a pipeline
		auto pipeline = make_unique<Pipeline>(*this, *producer);    // paras: (1) Executor (2) unique_ptr<ProducerToken>
		pipeline->sink = (PhysicalSink *)op;    // pipeline->sink = value of op, where op = 0x000060b00005ff70, then "(PhysicalSink *)" means its a address of a PhysicalSink
		pipeline->sink_state = pipeline->sink->GetGlobalState(context);
		if (parent) {
			// the parent is dependent on this pipeline to complete
			parent->AddDependency(pipeline.get());
		}
		switch (op->type) {     // op->type = SIMPLE_AGGREGATE
		case PhysicalOperatorType::CREATE_TABLE_AS:
		case PhysicalOperatorType::INSERT:
		case PhysicalOperatorType::DELETE_OPERATOR:
		case PhysicalOperatorType::UPDATE:
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::SIMPLE_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
		case PhysicalOperatorType::WINDOW:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::RESERVOIR_SAMPLE:
		case PhysicalOperatorType::TOP_N:
		case PhysicalOperatorType::COPY_TO_FILE:
			// single operator, set as child
			pipeline->child = op->children[0].get();
			break;
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::HASH_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::CROSS_PRODUCT:
			// regular join, create a pipeline with RHS source that sinks into this pipeline
			pipeline->child = op->children[1].get();
			// on the LHS (probe child), we recurse with the current set of pipelines
			BuildPipelines(op->children[0].get(), parent);
			break;
		case PhysicalOperatorType::DELIM_JOIN: {
			// duplicate eliminated join
			// create a pipeline with the duplicate eliminated path as source
			pipeline->child = op->children[0].get();
			break;
		}
		default:
			throw InternalException("Unimplemented sink type!");
		}
		// recurse into the pipeline child
		BuildPipelines(pipeline->child, pipeline.get());    // get from op->children
		for (auto &dependency : pipeline->GetDependencies()) {  // GetDependencies returns TYPE unordered_set<Pipeline *>
			auto dependency_cte = dependency->GetRecursiveCTE();    // cte=temporaray intermediate result which might be executed multiple times
			if (dependency_cte) {
				pipeline->SetRecursiveCTE(dependency_cte);
			}
		}
		if (op->type == PhysicalOperatorType::DELIM_JOIN) {
			// for delim joins, recurse into the actual join
			// any pipelines in there depend on the main pipeline
			auto &delim_join = (PhysicalDelimJoin &)*op;
			// any scan of the duplicate eliminated data on the RHS depends on this pipeline
			// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
			for (auto &delim_scan : delim_join.delim_scans) {
				delim_join_dependencies[delim_scan] = pipeline.get();
			}
			BuildPipelines(delim_join.join.get(), parent);
		}
		auto pipeline_cte = pipeline->GetRecursiveCTE();
		if (!pipeline_cte) {
			// regular pipeline: schedule it
			pipelines.push_back(move(pipeline));
		} else {
			// add it to the set of dependent pipelines in the CTE
			auto &cte = (PhysicalRecursiveCTE &)*pipeline_cte;
			cte.pipelines.push_back(move(pipeline));
		}
	} else {
		// operator is not a sink! recurse in children
		// first check if there is any additional action we need to do depending on the type
		switch (op->type) {
		case PhysicalOperatorType::DELIM_SCAN: {
			auto entry = delim_join_dependencies.find(op);
			D_ASSERT(entry != delim_join_dependencies.end());
			// this chunk scan introduces a dependency to the current pipeline
			// namely a dependency on the duplicate elimination pipeline to finish
			D_ASSERT(parent);
			parent->AddDependency(entry->second);
			break;
		}
		case PhysicalOperatorType::EXECUTE: {
			// EXECUTE statement: build pipeline on child
			auto &execute = (PhysicalExecute &)*op;
			BuildPipelines(execute.plan, parent);
			break;
		}
		case PhysicalOperatorType::RECURSIVE_CTE: {
			auto &cte_node = (PhysicalRecursiveCTE &)*op;
			// recursive CTE: we build pipelines on the LHS as normal
			BuildPipelines(op->children[0].get(), parent);
			// for the RHS, we gather all pipelines that depend on the recursive cte
			// these pipelines need to be rerun
			if (recursive_cte) {
				throw InternalException("Recursive CTE detected WITHIN a recursive CTE node");
			}
			recursive_cte = op;
			BuildPipelines(op->children[1].get(), parent);
			// re-order the pipelines such that they are executed in the correct order of dependencies
			for (idx_t i = 0; i < cte_node.pipelines.size(); i++) {
				auto &deps = cte_node.pipelines[i]->GetDependencies();
				for (idx_t j = i + 1; j < cte_node.pipelines.size(); j++) {
					if (deps.find(cte_node.pipelines[j].get()) != deps.end()) {
						// pipeline "i" depends on pipeline "j" but pipeline "i" is scheduled to be executed before
						// pipeline "j"
						std::swap(cte_node.pipelines[i], cte_node.pipelines[j]);
						i--;
						continue;
					}
				}
			}
			for (idx_t i = 0; i < cte_node.pipelines.size(); i++) {
				cte_node.pipelines[i]->ClearParents();
			}
			if (parent) {
				parent->SetRecursiveCTE(nullptr);
			}

			recursive_cte = nullptr;
			return;
		}
		case PhysicalOperatorType::RECURSIVE_CTE_SCAN: {
			if (!recursive_cte) {
				throw InternalException("Recursive CTE scan found without recursive CTE node");
			}
			if (parent) {
				// found a recursive CTE scan in a child pipeline
				// mark the child pipeline as recursive
				parent->SetRecursiveCTE(recursive_cte);
			}
			break;
		}
		default:
			break;
		}
		for (auto &child : op->children) {
			BuildPipelines(child.get(), parent);
		}
	}
}

vector<LogicalType> Executor::GetTypes() {
	D_ASSERT(physical_plan);
	return physical_plan->GetTypes();
}

void Executor::PushError(const string &exception) {
	lock_guard<mutex> elock(executor_lock);
	// interrupt execution of any other pipelines that belong to this executor
	context.interrupted = true;
	// push the exception onto the stack
	exceptions.push_back(exception);
}

void Executor::Flush(ThreadContext &tcontext) {
	lock_guard<mutex> elock(executor_lock);
	context.profiler.Flush(tcontext.profiler);
}

bool Executor::GetPipelinesProgress(int &current_progress) {
	lock_guard<mutex> elock(executor_lock);

	if (!pipelines.empty()) {
		return pipelines.back()->GetProgress(current_progress); //last elem in pipelines
	} else {
		current_progress = -1;
		return true;
	}
}

unique_ptr<DataChunk> Executor::FetchChunk() {
    //printf("Executor::FetchChunk \n");
	D_ASSERT(physical_plan);

	ThreadContext thread(context);
	TaskContext task;
	ExecutionContext econtext(context, thread, task);   // context, empty, empty

	auto chunk = make_unique<DataChunk>();
	// run the plan to get the next chunks ❓哪里update了下一个chunk？
	physical_plan->InitializeChunkEmpty(*chunk);    // update *chunk.data: initialize 3 empty elements(amount of types), put them in vector<Vector> chuck.data
	physical_plan->GetChunk(econtext, *chunk, physical_state.get());    //physical_state is modified in executor::Initialize
	physical_plan->FinalizeOperatorState(*physical_state, econtext);
	context.profiler.Flush(thread.profiler);
	return chunk;
}

} // namespace duckdb
