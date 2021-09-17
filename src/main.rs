use clap::{App, Arg, ArgMatches};

const PATH: &str = "PATH";

fn main() {
    let arguments = parse_arguments();
    let path = arguments.value_of(PATH).expect("Unable to parse arguments");
    match log_cruncher::crunch(path) {
        Ok(report) => println!("{}", report),
        Err(e) => println!("{:?}", e),
    }
}

#[derive(serde::Serialize)]
struct Record {
    r#type: String,
    foo: String,
    bar: Vec<i32>,
}

fn parse_arguments() -> ArgMatches<'static> {
    App::new(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name(PATH)
                .required(true)
                .short("p")
                .long("path")
                .takes_value(true)
                .help("Path to the file"),
        )
        .get_matches()
}
