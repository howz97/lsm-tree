use std::{path::PathBuf, time};

use clap::{command, Parser, ValueEnum};
use log::info;
use lsm_tree::{
    compact::{
        CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
        TieredCompactionOptions,
    },
    lsm_storage::{LsmStorageOptions, MiniLsm},
};
use rand::Rng;

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    Leveled,
    Tiered,
    None,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/tmp/lsm-tree")]
    path: PathBuf,
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,
    #[arg(long, default_value_t = false)]
    wal: bool,
    #[arg(long, default_value_t = false)]
    serial: bool,
    #[arg(short, long, default_value_t = 10000)]
    events: u32,
    #[arg(long, default_value_t = 0)]
    ratio: u8,
    #[arg(short, long, default_value_t = 1)]
    threads: u16,
    #[arg(long, default_value_t = 1)]
    loops: u16,
    #[arg(long, default_value_t = 4096)]
    blk_byts: u16,
    #[arg(long, default_value_t = 4096)]
    sst_kbs: u16,
    #[arg(long, default_value_t = 4)]
    mem_limit: u8,
}

fn bench(storage: &MiniLsm, events: u32, ratio: u8) {
    let gen_key = |i| format!("{:14}", i);
    let gen_value = |i| format!("{:114}", i);
    let mut rng = rand::thread_rng();
    for i in 0..events {
        let k = rng.gen::<u16>();
        let key = gen_key(k);
        if (k % 100) < ratio as u16 {
            let val = gen_value(i);
            storage.put(key.as_bytes(), val.as_bytes()).unwrap();
        } else {
            storage.get(key.as_bytes()).unwrap();
        }
    }
}

fn main() {
    env_logger::init();
    log::set_max_level(log::LevelFilter::Info);

    let args = Args::parse();
    let compact_opts = match args.compaction {
        CompactionStrategy::None => CompactionOptions::NoCompaction,
        CompactionStrategy::Simple => CompactionOptions::Simple(SimpleLeveledCompactionOptions {
            size_ratio_percent: 200,
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
        }),
        CompactionStrategy::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
            num_tiers: 3,
            max_size_amplification_percent: 200,
            size_ratio: 1,
            min_merge_width: 2,
        }),
        CompactionStrategy::Leveled => CompactionOptions::Leveled(LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 2,
            max_levels: 4,
            base_level_size_mb: 128,
            level_size_multiplier: 2,
        }),
    };
    let opts = LsmStorageOptions {
        block_size: args.blk_byts as usize,
        target_sst_size: args.sst_kbs as usize * 1024,
        num_memtable_limit: args.mem_limit as usize,
        compaction_options: compact_opts,
        enable_wal: args.wal,
        serializable: args.serial,
    };

    let storage = MiniLsm::open(&args.path, opts).unwrap();
    let events = args.events;
    let num_thd = args.threads as u32;
    info!("benchmark start...");
    for _ in 0..args.loops {
        let start = time::SystemTime::now();
        std::thread::scope(|s| {
            (0..num_thd).for_each(|_| {
                s.spawn(|| bench(&storage, events, args.ratio));
            })
        });
        let dur_secs = start.elapsed().unwrap().as_secs_f32();
        let ops = (events * num_thd) as f32 / dur_secs;
        info!("OPS: {ops}");
    }
    storage.dump_structure();
}
