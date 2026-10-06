use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize)]
pub struct Hit {
    pub page: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Count {
    pub page: String,
    pub total: u64,
}

/// What each page's cell remembers.
#[derive(Default, Serialize, Deserialize)]
pub struct Seen {
    total: u64,
}

/// How a visit changes it.
#[derive(Serialize, Deserialize)]
pub struct Counted;

impl Change<Seen> for Counted {
    fn apply_to(&self, seen: &mut Seen) {
        seen.total += 1;
    }
}

/// Counts the visits of one page and answers the new count.
///
/// The relay keys the channel by page, so each page has a cell of its own,
/// and `Seen` is that cell's state. The state changes only through the
/// `Counted` change, which is recorded as it happens, so a runner that
/// restarts finds the count it had.
#[derive(Default)]
pub struct CountHits;

// state is saved under this name, so choose it once
#[tasklet(name = "count")]
impl Tasklet for CountHits {
    type Input = Hit;
    type Output = Count;
    type State = Seen;
    type Change = Counted;

    fn run(&mut self, hit: Hit, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Counted);
        let total = ctx.state().total;
        ctx.emit(Count {
            page: hit.page,
            total,
        })
    }
}

/// Runs the tasklet with no controller: one harness per page stands in for
/// that page's cell.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use flezha_runner::testing::TaskletTestHarness;

    use super::*;

    fn count(page: &str, total: u64) -> Count {
        Count {
            page: page.into(),
            total,
        }
    }

    #[test]
    fn counts_each_page_on_its_own_and_survives_a_restart() {
        let mut cells: HashMap<&str, TaskletTestHarness<CountHits>> = HashMap::new();
        for page in ["home", "home", "about", "home"] {
            cells
                .entry(page)
                .or_insert_with(|| TaskletTestHarness::new().with_cell_key(page))
                .apply_inputs([Hit { page: page.into() }]);
        }

        // Replaying the recorded changes rebuilds the state each cell had, as a
        // restart does.
        for cell in cells.values() {
            cell.replay_equals()
                .expect("the replayed state equals the live one");
        }
        let mut cell = |page| cells.remove(page).expect("a cell per page");
        cell("home").expect_emits(&[count("home", 1), count("home", 2), count("home", 3)]);
        cell("about").expect_emits(&[count("about", 1)]);
    }
}
