import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LucideIcon } from "lucide-react";

interface SentimentCardProps {
  title: string;
  value: string;
  subtitle?: string;
  icon: LucideIcon;
  trend?: "up" | "down" | "neutral";
  sentiment?: "positive" | "negative" | "neutral";
}

export const SentimentCard = ({ 
  title, 
  value, 
  subtitle, 
  icon: Icon,
  trend,
  sentiment 
}: SentimentCardProps) => {
  const getSentimentColor = () => {
    if (sentiment === "positive") return "text-positive";
    if (sentiment === "negative") return "text-negative";
    return "text-muted-foreground";
  };

  return (
    <Card className="hover:shadow-lg transition-shadow">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
        <Icon className={`h-4 w-4 ${getSentimentColor()}`} />
      </CardHeader>
      <CardContent>
        <div className={`text-2xl font-bold ${getSentimentColor()}`}>{value}</div>
        {subtitle && (
          <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
        )}
      </CardContent>
    </Card>
  );
};
