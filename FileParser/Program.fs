open System.IO
open System
open FSharp.Collections.ParallelSeq

// Fixed fields in portfolio.csv
type PortfolioField = 
    | Trade = 0
    | CounterParty = 2
    | Type = 6
    | CCY = 8
    | Maturity = 13

// Splits a CSV string into a string array
let splitLine (line : string) = 
    line.Split [| ',' |] 
    |> Array.map (fun s -> s.Trim())

// Read a file into a sequence of strings removing comment lines
let readLines file = 
    file
    |> File.ReadAllLines
    |> PSeq.filter (fun s -> not (s.StartsWith "//"))

// Returns a multimap of a file keyed on supplied field id
let buildFileMap file field = 
    file
    |> readLines
    |> PSeq.map splitLine
    |> PSeq.groupBy (fun s -> s.[int field])
    |> Map.ofSeq

// Simple frequency histogram
let histogram list = 
    list
    |> PSeq.groupBy (fun a -> a)
    |> PSeq.map (fun (key, elements) -> key, PSeq.length elements)
    |> Map.ofSeq

// An index of -1 implies the last element
let bucketByTenor (valDate : System.DateTime) index tenor (lines : seq<string []>) = 
    lines
    |> PSeq.map (fun s -> DateTime.Parse s.[if index = -1 then s.Length - 1 else index])
    |> PSeq.filter (fun s -> s > valDate)
    |> PSeq.map (fun s -> (s - valDate).Days / tenor)
    |> histogram

[<EntryPoint>]
let main argv = 

    let stopWatch = System.Diagnostics.Stopwatch.StartNew()

    let dir = @"/Users/richard_lewis/Dropbox/nab/Service/"
    let portfolioFile = dir + @"portfolio.csv"
    let scheduleFile = dir + @"schedules.csv"
    let marketDataFile = dir + @"marketData.csv"
    let outputFile = dir + @"statistics.log"

    let portMap = buildFileMap portfolioFile

    let portByTrade = portMap PortfolioField.Trade // multimap keyed by trade id
    let portByType = portMap PortfolioField.Type // multimap keyed by product type
    let portByCP = portMap PortfolioField.CounterParty // multimap keyed by counterparty
    let portByCCY = portMap PortfolioField.CCY // multimap keyed by currency

    // Create a distribution by product type
    let tradeDistPercent = 
        portByType 
        |> Map.map (fun k v -> float (PSeq.length v) / float portByTrade.Count * 100.0)

    // TODO: change this to break as soon as NumeraireRatio is found
    let valDate = 
        marketDataFile
        |> File.ReadAllLines
        |> PSeq.filter (fun s -> s.StartsWith "NumeraireRatio")
        |> PSeq.map splitLine
        |> PSeq.map (fun s -> s.[2])
        |> PSeq.head
        |> DateTime.Parse
    
    let fxfMaturityDist = 
        portByType.["FXForward"] 
        |> bucketByTenor valDate (int PortfolioField.Maturity) 30
    
    let swapMaturityDist = 
        scheduleFile
        |> File.ReadAllLines
        |> PSeq.map (fun s -> s.Trim()) // trim white spaces
        |> PSeq.filter (fun s -> s.StartsWith("adtEnd")) // filter out everything that does not start with adtEnd
        |> PSeq.map (fun s -> s.Split [| ',' |]) // split on commas
        |> bucketByTenor valDate -1 365 // bucket yearly
    
    let cumSum =
        List.scan (+) 0 >> List.tail

    let outFile = new StreamWriter(outputFile)

    outFile.WriteLine(sprintf "# of Trades: %d" portByTrade.Count)
    outFile.WriteLine(sprintf "# of Currencies: %d" portByCCY.Count)
    outFile.WriteLine(sprintf "# of Counterparties: %d" portByCP.Count)
     
    outFile.WriteLine ""
    outFile.WriteLine "Trade Distribution"

    outFile.WriteLine ""
    for (KeyValue(k, v)) in tradeDistPercent do
        outFile.WriteLine(sprintf "%s: %0.2f%%" k v)

    outFile.WriteLine ""
    outFile.WriteLine "FX Forward Maturity Distribution (in Months)"
    for (KeyValue(k, v)) in fxfMaturityDist do
        outFile.WriteLine(sprintf "%d,%d" k v)

    outFile.WriteLine ""
    outFile.WriteLine "Swap Maturity Distribution (in Years)"
    for (KeyValue(k, v)) in swapMaturityDist do
        outFile.WriteLine(sprintf "%d,%d" k v)

    stopWatch.Stop()
    outFile.WriteLine(sprintf "Elapsed Time in (sec) %f" (stopWatch.Elapsed.TotalMilliseconds / 1000.))

    outFile.Close()
    0
