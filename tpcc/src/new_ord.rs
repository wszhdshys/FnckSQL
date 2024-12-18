use crate::load::{nu_rand, CUST_PER_DIST, DIST_PER_WARE, MAX_ITEMS, MAX_NUM_ITEMS};
use crate::{other_ware, TpccArgs, TpccError, TpccTest, TpccTransaction, ALLOW_MULTI_WAREHOUSE_TX};
use chrono::Utc;
use fnck_sql::db::DBTransaction;
use fnck_sql::storage::Storage;
use rand::prelude::ThreadRng;
use rand::Rng;
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct NewOrdArgs {
    joins: bool,
    w_id: usize,
    d_id: usize,
    c_id: usize,
    o_ol_cnt: usize,
    o_all_local: u8,
    item_id: Vec<usize>,
    supware: Vec<usize>,
    qty: Vec<u8>,
}

impl NewOrdArgs {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        joins: bool,
        w_id: usize,
        d_id: usize,
        c_id: usize,
        o_ol_cnt: usize,
        o_all_local: u8,
        item_id: Vec<usize>,
        supware: Vec<usize>,
        qty: Vec<u8>,
    ) -> Self {
        Self {
            joins,
            w_id,
            d_id,
            c_id,
            o_ol_cnt,
            o_all_local,
            item_id,
            supware,
            qty,
        }
    }
}

pub(crate) struct NewOrd;
pub(crate) struct NewOrdTest;

impl<S: Storage> TpccTransaction<S> for NewOrd {
    type Args = NewOrdArgs;

    fn run(tx: &mut DBTransaction<S>, args: &Self::Args) -> Result<(), TpccError> {
        let mut price = vec![Decimal::default(); MAX_NUM_ITEMS];
        let mut iname = vec![String::new(); MAX_NUM_ITEMS];
        let mut stock = vec![0; MAX_NUM_ITEMS];
        let mut bg = vec![String::new(); MAX_NUM_ITEMS];
        let mut amt = vec![Decimal::default(); MAX_NUM_ITEMS];
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let (c_discount, c_last, c_credit, w_tax) = if args.joins {
            // "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?"
            let (_, tuple) = tx.run(format!("SELECT c.c_discount, c.c_last, c.c_credit, w.w_tax FROM customer AS c JOIN warehouse AS w ON c.c_w_id = w_id AND w.w_id = {} AND c.c_w_id = {} AND c.c_d_id = {} AND c.c_id = {}", args.w_id, args.w_id, args.d_id, args.c_id))?;
            let c_discount = tuple[0].values[0].decimal().unwrap();
            let c_last = tuple[0].values[1].utf8().unwrap();
            let c_credit = tuple[0].values[2].utf8().unwrap();
            let w_tax = tuple[0].values[3].decimal().unwrap();

            (c_discount, c_last, c_credit, w_tax)
        } else {
            // "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
            let (_, tuple) = tx.run(format!("SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", args.w_id, args.d_id, args.c_id))?;
            let c_discount = tuple[0].values[0].decimal().unwrap();
            let c_last = tuple[0].values[1].utf8().unwrap();
            let c_credit = tuple[0].values[2].utf8().unwrap();
            // "SELECT w_tax FROM warehouse WHERE w_id = ?"
            let (_, tuple) = tx.run(format!(
                "SELECT w_tax FROM warehouse WHERE w_id = {}",
                args.w_id
            ))?;
            let w_tax = tuple[0].values[0].decimal().unwrap();

            (c_discount, c_last, c_credit, w_tax)
        };
        // "SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE"
        let (_, tuple) = tx.run(format!(
            "SELECT d_next_o_id, d_tax FROM district WHERE d_id = {} AND d_w_id = {}",
            args.d_id, args.w_id
        ))?;
        let d_next_o_id = tuple[0].values[0].i32().unwrap();
        let d_tax = tuple[0].values[1].decimal().unwrap();
        // "UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?"
        let _ = tx.run(format!(
            "UPDATE district SET d_next_o_id = {} + 1 WHERE d_id = {} AND d_w_id = {}",
            d_next_o_id, args.d_id, args.w_id
        ))?;
        let o_id = d_next_o_id;
        // "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)"
        let _ = tx.run(format!("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES({}, {}, {}, {}, '{}', {}, {})", o_id, args.d_id, args.w_id, args.c_id, now, args.o_ol_cnt, args.o_all_local))?;
        // "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)
        let _ = tx.run(format!(
            "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES ({},{},{})",
            o_id, args.d_id, args.w_id
        ))?;
        let mut ol_num_seq = vec![0; MAX_NUM_ITEMS];

        for i in 0..args.o_ol_cnt {
            ol_num_seq[i] = i;
        }
        for i in 0..args.o_ol_cnt - 1 {
            let mut tmp_0 =
                (MAX_ITEMS + 1) * args.supware[ol_num_seq[i]] + args.item_id[ol_num_seq[i]];
            let mut min_num = i;
            for j in i + 1..args.o_ol_cnt {
                let tmp_1 =
                    (MAX_ITEMS + 1) * args.supware[ol_num_seq[j]] + args.item_id[ol_num_seq[j]];
                if tmp_1 < tmp_0 {
                    tmp_0 = tmp_1;
                    min_num = j;
                }
            }
            if min_num != i {
                let swp = ol_num_seq[min_num];
                ol_num_seq[min_num] = ol_num_seq[i];
                ol_num_seq[i] = swp;
            }
        }
        for ol_number in 1..args.o_ol_cnt + 1 {
            let ol_supply_w_id = args.supware[ol_num_seq[ol_number - 1]];
            let ol_i_id = args.item_id[ol_num_seq[ol_number - 1]];
            let ol_quantity = args.qty[ol_num_seq[ol_number - 1]];
            // "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?"
            let (_, tuples) = tx.run(format!(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",
                ol_i_id
            ))?;
            if tuples.is_empty() {
                return Err(TpccError::EmptyTuples);
            }
            let i_price = tuples[0].values[0].decimal().unwrap();
            let i_name = tuples[0].values[1].utf8().unwrap();
            let i_data = tuples[0].values[2].utf8().unwrap();

            price[ol_num_seq[ol_number - 1]] = i_price;
            iname[ol_num_seq[ol_number - 1]] = i_name;

            // "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE"
            let (_, tuples) = tx.run(format!("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = {} AND s_w_id = {}", ol_i_id, ol_supply_w_id))?;
            let mut s_quantity = tuples[0].values[0].i16().unwrap();
            let s_data = tuples[0].values[1].utf8().unwrap();
            let s_dist_01 = tuples[0].values[2].utf8().unwrap();
            let s_dist_02 = tuples[0].values[3].utf8().unwrap();
            let s_dist_03 = tuples[0].values[4].utf8().unwrap();
            let s_dist_04 = tuples[0].values[5].utf8().unwrap();
            let s_dist_05 = tuples[0].values[6].utf8().unwrap();
            let s_dist_06 = tuples[0].values[7].utf8().unwrap();
            let s_dist_07 = tuples[0].values[8].utf8().unwrap();
            let s_dist_08 = tuples[0].values[9].utf8().unwrap();
            let s_dist_09 = tuples[0].values[10].utf8().unwrap();
            let s_dist_10 = tuples[0].values[11].utf8().unwrap();

            let ol_dist_info = pick_dist_info(
                args.d_id, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
                s_dist_07, s_dist_08, s_dist_09, s_dist_10,
            );
            stock[ol_num_seq[ol_number - 1]] = s_quantity;
            bg[ol_num_seq[ol_number - 1]] =
                if i_data.contains("original") && s_data.contains("original") {
                    "B"
                } else {
                    "C"
                }
                .to_string();
            s_quantity = if s_quantity > ol_quantity as i16 {
                s_quantity - ol_quantity as i16
            } else {
                s_quantity - ol_quantity as i16 + 91
            };
            // "UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?"
            let _ = tx.run(format!(
                "UPDATE stock SET s_quantity = {} WHERE s_i_id = {} AND s_w_id = {}",
                s_quantity, ol_i_id, ol_supply_w_id
            ))?;

            // Tips: Integers always have 7 digits, so divide by 10 here
            let mut ol_amount = Decimal::from(ol_quantity)
                * i_price
                * (Decimal::from(1) + w_tax + d_tax)
                * (Decimal::from(1) - c_discount).round_dp(2);
            while ol_amount.mantissa() > 4 {
                ol_amount = ol_amount / Decimal::from(10);
            }

            amt[ol_num_seq[ol_number - 1]] = ol_amount;
            // "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            let _ = tx.run(format!("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, '{}')", o_id, args.d_id, args.w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info))?;
        }

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for NewOrdTest {
    fn name(&self) -> &'static str {
        "New-Order"
    }

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut DBTransaction<S>,
        num_ware: usize,
        args: &TpccArgs,
    ) -> Result<(), TpccError> {
        let mut all_local = 1;
        let notfound = MAX_ITEMS + 1;

        let mut itemid = vec![0; MAX_NUM_ITEMS];
        let mut supware = vec![0; MAX_NUM_ITEMS];
        let mut qty = vec![0; MAX_NUM_ITEMS];

        let w_id = rng.gen_range(0..num_ware) + 1;
        let d_id = rng.gen_range(1..DIST_PER_WARE);
        let c_id = nu_rand(rng, 1023, 1, CUST_PER_DIST);
        let ol_cnt = rng.gen_range(5..15);
        let rbk = rng.gen_range(1..100);

        for i in 0..ol_cnt {
            itemid[i] = nu_rand(rng, 8191, 1, MAX_ITEMS);
            if (i == ol_cnt - 1) && (rbk == 1) {
                itemid[i] = notfound;
            }
            if ALLOW_MULTI_WAREHOUSE_TX {
                if rng.gen_range(1..100) != 1 {
                    supware[i] = w_id;
                } else {
                    supware[i] = other_ware(rng, w_id, num_ware);
                    all_local = 0;
                }
            } else {
                supware[i] = w_id;
            }
            qty[i] = rng.gen_range(1..10);
        }
        let args = NewOrdArgs::new(
            args.joins, w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty,
        );
        NewOrd::run(tx, &args)?;

        Ok(())
    }
}

fn pick_dist_info(
    ol_supply_w_id: usize,
    s_dist_01: String,
    s_dist_02: String,
    s_dist_03: String,
    s_dist_04: String,
    s_dist_05: String,
    s_dist_06: String,
    s_dist_07: String,
    s_dist_08: String,
    s_dist_09: String,
    s_dist_10: String,
) -> String {
    match ol_supply_w_id {
        1 => s_dist_01,
        2 => s_dist_02,
        3 => s_dist_03,
        4 => s_dist_04,
        5 => s_dist_05,
        6 => s_dist_06,
        7 => s_dist_07,
        8 => s_dist_08,
        9 => s_dist_09,
        10 => s_dist_10,
        _ => unreachable!(),
    }
}
