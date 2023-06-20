// The template class inherits from ROOT::Detail::RDF::RActionImpl
template <typename TH>
class OversampledTH : public ROOT::Detail::RDF::RActionImpl<OversampledTH<TH>> {
public:
  // Define the type of the histogram
  using Result_t = TH;

private:
  // Number of slots (threads)
  int nSlots;
  // Use unordered_map instead of map for faster access times
  std::unordered_map<long int, std::unordered_map<int, TH>> fHistos;
  // Shared pointer to the final histogram
  std::shared_ptr<TH> fFinalHisto;
  // Vector to keep track of the current event per slot
  std::vector<long int> current;
  // Keep track of the last flushed event
  long int lastFlush = -1;


public:
  // Constructor with parameters for histogram creation
  // The initialization list is used for more efficient variable initialization
  OversampledTH(std::string_view name, std::string_view title, int nbin, double xmin, double xmax)
    : nSlots(ROOT::IsImplicitMTEnabled() ? ROOT::GetThreadPoolSize() : 1),
      current(nSlots, -1),
      fFinalHisto(std::make_shared<TH>(std::string(name).c_str(), std::string(title).c_str(), nbin, xmin, xmax)) {
    cout << "nSlots: " << nSlots << endl;
  }

  // Move constructor and deletion of copy constructor
  OversampledTH(OversampledTH &&) = default;
  OversampledTH(const OversampledTH &) = delete;

  // Function to return the pointer to the final histogram
  std::shared_ptr<TH> GetResultPtr() const { return fFinalHisto; }

  // Initialize() and InitTask() functions are not used in this implementation
  void Initialize() {}
  void InitTask(TTreeReader *, unsigned int) {}

  // Exec function is called for each event and fills the histogram
  // The unordered_map for each event is filled only if it doesn't exist yet
  // Then, it checks if the event has changed, and if so, it flushes the histograms
  template <typename... ColumnTypes>
  void Exec(unsigned int slot, unsigned long genEvent, ColumnTypes... values, float weight = 1) {
    if (!fFinalHisto) {
      std::cerr << "Error: fFinalHisto is null" << std::endl;
      return;
    }
    if (!(fHistos[genEvent].find(slot) == fHistos[genEvent].end())) {
      fHistos[genEvent].emplace(slot, *(TH *)fFinalHisto->Clone());
      fHistos[genEvent][slot].Reset();
    }
    fHistos[genEvent][slot].Fill(values..., weight);
    cout << "genEvent: " << genEvent << " Slot: " << slot << endl;
    if (genEvent != current[slot]) {
      Flush();
      current[slot] = genEvent;
    }
  }

  // Function to fill the final histogram with the values from the different slots
  // The for loop uses a const reference to avoid unnecessary copies
  void fillOversampledHisto(const std::unordered_map<int, TH> &histosFromSlots) {
    for (const auto &kv : histosFromSlots) {
      auto histo = kv.second;
      fFinalHisto->Add(&histo);
    }
  }

  // Flush function to clean up the filled histograms
  // It takes an optional 'all' argument to decide whether to flush all histograms or not
  void Flush(bool all = false) {
    // Gets the minimum genEvent from all the slots
    auto minGen = *std::min_element(current.begin(), current.end());
    // If the last flushed genEvent is less than (minGen - 1) or 'all' is true, it starts flushing histograms
    // 'all' would be true when Finalize() function is called at the end of event loop
    if ((lastFlush < (minGen - 1)) || all) {
      // Iterate through fHistos
      for (auto it = fHistos.begin(); it != fHistos.end();) {
        auto genEvent = it->first;
        // If the genEvent is less than minGen, it means this event has been processed by all slots
        // So it's safe to fill the final histogram with this event's data and then remove it from fHistos
        if (genEvent < minGen) {
          fillOversampledHisto(it->second);
          lastFlush = genEvent;
          // Erase this event from fHistos. Since erasing invalidates the iterator,
          // we use the returned iterator which points to the next valid item
          it = fHistos.erase(it);
        } else {
          // If this event is not ready to be flushed, move to the next event
          ++it;
        }
      }
    }
  }

  void Finalize() {
    Flush(true); 
  }

  std::string GetActionName() { return "OversampledTH"; }
};

void df018_customActionsGPT4() {

  ROOT::EnableImplicitMT();

  ROOT::RDataFrame rdf{"Events", "test_oversampling.root"};

  auto dd = rdf.Define("FirstJet_pt", "Jet_pt[0]");

  using OversampledTH1F = OversampledTH<TH1F>;

  OversampledTH1F helper{"myTH1F",                // Name
                         "Oversampled Histogram", // Title
                         20, 0., 100.};           // Bins and range

  // We book the action: it will be treated during the event loop.
  auto myTH1F = dd.Book<long, float>(
      std::move(helper), {"genEventProgressiveNumber", "FirstJet_pt"});

  myTH1F->Print();
}
