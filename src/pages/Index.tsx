import { useState } from "react";
import { TrendingUp, TrendingDown, MessageSquare, Hash, SmilePlus, Frown } from "lucide-react";
import { SentimentCard } from "@/components/SentimentCard";
import { SentimentChart } from "@/components/SentimentChart";
import { TrendChart } from "@/components/TrendChart";
import { WordCloudDisplay } from "@/components/WordCloudDisplay";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

const Index = () => {
  const [selectedCountry, setSelectedCountry] = useState("arab");
  const [selectedTopic, setSelectedTopic] = useState("SDG01");

  // Mock data - will be replaced with API calls
  const mockStats = {
    total: "45,231",
    sdgTotal: "12,456",
    max: { value: "3,421", sdg: "SDG01" },
    min: { value: "234", sdg: "SDG17" },
    max_pos: "SDG03",
    max_neg: "SDG16"
  };

  const mockSentiment = {
    positive: 4520,
    negative: 1230,
    neutral: 6706
  };

  const mockTrends = [
    { day: "2024-01-01", count: 450 },
    { day: "2024-01-02", count: 520 },
    { day: "2024-01-03", count: 480 },
    { day: "2024-01-04", count: 650 },
    { day: "2024-01-05", count: 720 },
    { day: "2024-01-06", count: 690 },
    { day: "2024-01-07", count: 800 }
  ];

  const mockWordCloud = [
    { word: "poverty", count: 450 },
    { word: "education", count: 380 },
    { word: "health", count: 320 },
    { word: "water", count: 280 },
    { word: "energy", count: 250 },
    { word: "jobs", count: 220 },
    { word: "inequality", count: 190 },
    { word: "climate", count: 180 },
    { word: "peace", count: 160 },
    { word: "partnership", count: 140 }
  ];

  const countries = [
    { code: "arab", name: "Arab Region" },
    { code: "lb", name: "Lebanon" },
    { code: "eg", name: "Egypt" },
    { code: "jo", name: "Jordan" },
    { code: "sa", name: "Saudi Arabia" }
  ];

  const topics = [
    { id: "SDG01", name: "No Poverty" },
    { id: "SDG02", name: "Zero Hunger" },
    { id: "SDG03", name: "Good Health" },
    { id: "SDG04", name: "Quality Education" },
    { id: "SDG05", name: "Gender Equality" }
  ];

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b bg-card/50 backdrop-blur-sm sticky top-0 z-50">
        <div className="container mx-auto px-4 py-6">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-primary-glow bg-clip-text text-transparent">
                SDG Sentiment Analytics
              </h1>
              <p className="text-muted-foreground mt-1">
                Real-time social media sentiment analysis for Sustainable Development Goals
              </p>
            </div>
            <div className="flex gap-3">
              <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                <SelectTrigger className="w-[180px]">
                  <SelectValue placeholder="Select country" />
                </SelectTrigger>
                <SelectContent>
                  {countries.map((country) => (
                    <SelectItem key={country.code} value={country.code}>
                      {country.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={selectedTopic} onValueChange={setSelectedTopic}>
                <SelectTrigger className="w-[200px]">
                  <SelectValue placeholder="Select topic" />
                </SelectTrigger>
                <SelectContent>
                  {topics.map((topic) => (
                    <SelectItem key={topic.id} value={topic.id}>
                      {topic.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-8">
        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <SentimentCard
            title="Total Tweets"
            value={mockStats.total}
            subtitle="All topics combined"
            icon={MessageSquare}
          />
          <SentimentCard
            title="SDG Tweets"
            value={mockStats.sdgTotal}
            subtitle="Across all SDGs"
            icon={Hash}
          />
          <SentimentCard
            title="Most Positive SDG"
            value={mockStats.max_pos}
            subtitle="Highest positive sentiment"
            icon={SmilePlus}
            sentiment="positive"
          />
          <SentimentCard
            title="Most Negative SDG"
            value={mockStats.max_neg}
            subtitle="Highest negative sentiment"
            icon={Frown}
            sentiment="negative"
          />
        </div>

        {/* Charts Row */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <SentimentChart data={mockSentiment} />
          <TrendChart data={mockTrends} title="Tweet Volume Over Time" />
        </div>

        {/* Word Cloud */}
        <div className="mb-8">
          <WordCloudDisplay words={mockWordCloud} />
        </div>

        {/* Additional Stats */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <SentimentCard
            title="Most Discussed SDG"
            value={mockStats.max.sdg}
            subtitle={`${mockStats.max.value} tweets`}
            icon={TrendingUp}
            sentiment="positive"
          />
          <SentimentCard
            title="Least Discussed SDG"
            value={mockStats.min.sdg}
            subtitle={`${mockStats.min.value} tweets`}
            icon={TrendingDown}
            sentiment="neutral"
          />
        </div>
      </main>
    </div>
  );
};

export default Index;
