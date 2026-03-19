# This line enables future annotations, allowing us to use types that are defined later in the code without needing string literals.
from __future__ import annotations

from data_gov_datasets_explorer.db import init_db
from data_gov_datasets_explorer.crawler.crawler_helpers import fetch_datasets, persist_dataset_with_retry, start_run, finish_run
from data_gov_datasets_explorer.logger import AppLogger


log = AppLogger("crawler")


def main() -> None:
    log.info("Step 1/6: Initializing database metadata")
    init_db()

    log.info("Step 2/6: Fetching datasets from data.gov")
    datasets = fetch_datasets()
    log.info(f"Fetched {len(datasets)} datasets")

    log.info("Step 3/6: Creating ingestion run")
    run_id = start_run(total_target=len(datasets))
    log.info(f"Run created with id={run_id}")

    total_success = 0
    total_failed = 0
    failure_samples: list[str] = []

    try:
        log.info("Step 4/6: Processing datasets with retry policy")
        for index, dataset_payload in enumerate(datasets, start=1):
            ok, attempts, last_error, dataset_id = persist_dataset_with_retry(run_id, dataset_payload)
            if ok:
                total_success += 1
                log.info(
                    f"Processed dataset {index}/{len(datasets)}: id={dataset_id}, status=success, attempts={attempts}"
                )
            else:
                total_failed += 1
                log.warning(
                    f"Processed dataset {index}/{len(datasets)}: id={dataset_id}, status=failed, attempts={attempts}"
                )
                if last_error and len(failure_samples) < 5:
                    failure_samples.append(f"{dataset_id} (attempts={attempts}): {last_error}")

        log.info("Step 5/6: Finalizing ingestion run")
        finish_run(run_id, total_success=total_success, total_failed=total_failed)
    except Exception as exc:
        log.exception("Ingestion failed unexpectedly while processing datasets")
        finish_run(
            run_id,
            total_success=total_success,
            total_failed=total_failed,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        raise

    log.info("Step 6/6: Printing run summary")
    print("Run summary")
    print(f"Run ID: {run_id}")
    print(f"Target datasets: {len(datasets)}")
    print(f"Succeeded: {total_success}")
    print(f"Failed: {total_failed}")
    if failure_samples:
        print("Sample failures:")
        for item in failure_samples:
            print(f"- {item}")


if __name__ == "__main__":
    main()
