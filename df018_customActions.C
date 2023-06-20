template <typename TH>
class OversampledTH : public ROOT::Detail::RDF::RActionImpl<OversampledTH<TH>> {
public:
  // This type is a requirement for every helper.
  using Result_t = TH;

private:
  std::map<long int, std::map<int, TH>>
      fHistos; // (One histo per slot) per genEvent
  std::shared_ptr<TH> fFinalHisto;
  std::vector<long int> current; // current genEvent for each slot
  long int lastFlush = -1;       // default = -1 since genEvent starts from 0
  int nSlots;

public:
  // This constructor takes all the parameters needed to create a TH
  // TODO: Generalize (at least) to TH2 and TH3
  OversampledTH(std::string_view name, std::string_view title, int nbin,
                double xmin, double xmax) {
    nSlots = ROOT::IsImplicitMTEnabled() ? ROOT::GetThreadPoolSize() : 1;
    fFinalHisto =
        std::make_shared<TH>(std::string(name).c_str(),
                             std::string(title).c_str(), nbin, xmin, xmax);
  }
  // Move constructor
  OversampledTH(OversampledTH &&) = default;
  OversampledTH(const OversampledTH &) = delete;
  // Result pointer
  std::shared_ptr<TH> GetResultPtr() const { return fFinalHisto; }
  // Optional
  void Initialize() {}
  void InitTask(TTreeReader *, unsigned int) {}
  // Exec is called in the event loop for only one slot
  template <typename... ColumnTypes>
  void Exec(unsigned int slot, unsigned long genEvent, ColumnTypes... values,
            float weight = 1) { // ? weight=1 by default ?
    // If slot is not in the map, we create a new entry
    if (!(fHistos[genEvent].find(slot) == fHistos[genEvent].end())) {
      fHistos[genEvent].insert({slot, *(TH *)fFinalHisto->Clone()});
      fHistos[genEvent][slot].Reset(); // ! Check if it works
    }
    // Slot-histogram filling for a given genEvent
    // ? Add inner loop on RVec in Exec
    fHistos[genEvent][slot].Fill(values..., weight);
    // ?
    if (genEvent != current[slot]) {
      Flush();
      current[slot] = genEvent;
    }
  }
  void fillOversampledHisto(const std::map<int, TH> &histosFromSlots) {
    // Accumulate histograms from all slots
    for (auto &kv : histosFromSlots) {
      auto histo = kv.second;
      fFinalHisto->Add(&histo); // ? Sumw2
    }
  }
  void Flush(bool all = false) {
    // Get the minimum genEvent from all the slots
    auto minGen = *std::min_element(current.begin(), current.end());
    // When (minGen - 1) gets bigger than lastFlush, we flush all the
    // histograms with genEvent < minGen and we remove them from the map
    if ((lastFlush < (minGen - 1)) || all) {
      for (auto &kv : fHistos) {
        auto genEvent = kv.first;
        if (genEvent < minGen) {
          fillOversampledHisto(kv.second);
          lastFlush = genEvent;
          fHistos.erase(genEvent);
        }
      }
    }
  }
  void Finalize() {
    Flush(true); // Flush all
  }
  std::string GetActionName() { return "OversampledTH"; }
};

void df018_customActions() {

  ROOT::EnableImplicitMT();

  ROOT::RDataFrame dd{"Events", "~/test_oversampling.root"};

  using OversampledTH1F = OversampledTH<TH1F>;

  OversampledTH1F helper{"myTH1F",                // Name
                         "Oversampled Histogram", // Title
                         20, 0., 100.};           // Bins and range

  // We book the action: it will be treated during the event loop.
  auto myTH1F = dd.Book<long int, ROOT::VecOps::RVec<float>>(
      std::move(helper), {"genEventProgressiveNumber", "Jet_pt"});

  myTH1F->Print();
}
