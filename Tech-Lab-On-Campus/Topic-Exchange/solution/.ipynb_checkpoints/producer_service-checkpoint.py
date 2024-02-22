import argparse
import sys

import producer_sol

def main(ticker: str, price: float, sector: str) -> None:
    marketWatch = producer_sol.mqProducer("Market Stock Exchange")

    stock = f"{ticker} {price}"

    marketWatch.publishOrder(name=ticker, sector=sector, message=stock)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process Stock Name, Price And Type."
    )

    parser.add_argument(
        "-t", "--ticker", type=str, help="Stock Ticker", required=True
    )
    parser.add_argument(
        "-p", "--price", type=float, help="Stock Price", required=True
    )
    parser.add_argument(
        "-s", "--sector", type=str, help="Stock Sector", required=True
    )

    args = parser.parse_args()

    sys.exit(main(args.ticker, args.price, args.sector))