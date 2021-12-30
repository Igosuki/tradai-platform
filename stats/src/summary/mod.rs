#![allow(dead_code)]
use trading::position::Position;

pub mod data;
pub mod drawdown;
pub mod pnl;
pub mod tradings;

pub trait PositionSummariser {
    fn update(&mut self, position: &Position);
    fn generate_summary(&mut self, positions: &[Position]) {
        for position in positions {
            self.update(position);
        }
    }
}

pub trait TablePrinter {
    fn print(&self);
}
