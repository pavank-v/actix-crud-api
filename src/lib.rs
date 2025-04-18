use std::{fmt, str::FromStr, sync::Arc};

use chrono::NaiveDateTime;
use heed::{types::*, Database, Env};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum Status {
    Active,
    Inactive,
    Pending,
    Closed,
    UnderReview
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Status::Active => "Active",
            Status::Closed => "Closed",
            Status::Pending => "Pending",
            Status::Inactive => "Inactive",
            Status::UnderReview => "UnderReview"
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Status {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(Status::Active),
            "closed" => Ok(Status::Closed),
            "pending" => Ok(Status::Pending),
            "inactive" => Ok(Status::Inactive),
            "underreview" => Ok(Status::UnderReview),
            _ => Err(())
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct DBSchema {
    pub permit_link: String,
    pub permit_number: String,
    pub client: String,
    pub opened: NaiveDateTime,
    pub last_updated: NaiveDateTime,
    pub status_updated: NaiveDateTime,
    pub county: String,
    pub county_status: Status,
    pub manual_status: Status,
    pub address: String
}

#[derive(Clone)]
pub struct DBHandles {
    pub main_db: Database<Str, SerdeBincode<DBSchema>>,
    pub county_index: Database<Str, Str>,
    pub opened_index: Database<SerdeBincode<NaiveDateTime>, Str>,
    pub last_updated: Database<SerdeBincode<NaiveDateTime>, Str>,
    pub county_status: Database<Str, Str>
}

pub fn setup_db(env: Arc<Env>) -> Result<DBHandles, Box<dyn std::error::Error>> {
    let db_path = std::path::Path::new("database");

    if !db_path.exists() {
        std::fs::create_dir_all(db_path)?;
    }

    {
        let mut wtxn = env.write_txn()?;
        
        if env
            .open_database::<Str, SerdeBincode<DBSchema>>(&wtxn, Some("main_db"))?
            .is_none()
        {
            println!("Creating main_db...");
            env.create_database::<Str, SerdeBincode<DBSchema>>(&mut wtxn, Some("main_db"))?;
        }

        if env
            .open_database::<Str, Str>(&wtxn, Some("county_index"))?
            .is_none()
        {
            println!("Creating county_index...");
            env.create_database::<Str, Str>(&mut wtxn, Some("county_index"))?;
        }

        if env
            .open_database::<SerdeBincode<NaiveDateTime>, Str>(&wtxn, Some("opened_index"))?
            .is_none()
        {
            println!("Creating opened_index...");
            env.create_database::<SerdeBincode<NaiveDateTime>, Str>(&mut wtxn, Some("opened_index"))?;
        }

        if env
            .open_database::<SerdeBincode<NaiveDateTime>, Str>(&wtxn, Some("last_updated"))?
            .is_none()
        {
            println!("Creating last_updated...");
            env.create_database::<SerdeBincode<NaiveDateTime>, Str>(&mut wtxn, Some("last_updated"))?;
        }

        if env
            .open_database::<Str, Str>(&wtxn, Some("county_status"))?
            .is_none()
        {
            println!("Creating county_status...");
            env.create_database::<Str, Str>(&mut wtxn, Some("county_status"))?;
        }
        
        wtxn.commit()?
    }

    let rtxn = env.read_txn()?;

    let main_db = env
        .open_database(&rtxn, Some("main_db"))?
        .unwrap();
    let county_index = env
        .open_database(&rtxn, Some("county_index"))?
        .unwrap();
    let opened_index = env
        .open_database(&rtxn, Some("opened_index"))?
        .unwrap();
    let last_updated = env
        .open_database(&rtxn, Some("last_updated"))?
        .unwrap();
    let county_status = env
        .open_database(&rtxn, Some("county_status"))?
        .unwrap();
    
    drop(rtxn);
        
    Ok(DBHandles {
        main_db,
        county_index,
        opened_index,
        last_updated,
        county_status
    })
}