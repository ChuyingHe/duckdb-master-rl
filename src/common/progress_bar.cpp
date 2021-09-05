#include "duckdb/common/progress_bar.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void ProgressBar::ProgressBarThread() {
#ifndef DUCKDB_NO_THREADS   /*if DUCKDB_NO_THREADS is not defined */
	WaitFor(std::chrono::milliseconds(show_progress_after)); /*in client_context.hpp: show_progress_after = 2000*/
	while (!stop) {
		int new_percentage;
		supported = executor->GetPipelinesProgress(new_percentage);
		current_percentage = new_percentage;
		if (supported && current_percentage > -1 && executor->context.print_progress_bar) {
			Printer::PrintProgress(current_percentage, PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		}
		WaitFor(std::chrono::milliseconds(time_update_bar));
	}
#endif
}

int ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
#ifndef DUCKDB_NO_THREADS
	current_percentage = 0;
	progress_bar_thread = thread(&ProgressBar::ProgressBarThread, this);
#endif
}

void ProgressBar::Stop() {
#ifndef DUCKDB_NO_THREADS
	if (progress_bar_thread.joinable()) {
		stop = true;
		c.notify_one(); //randomly notify one waiting thread
		progress_bar_thread.join(); //check ob fertig ist
		if (supported && current_percentage > 0 && executor->context.print_progress_bar) {
			Printer::FinishProgressBarPrint(PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		}
	}
#endif
}
} // namespace duckdb
