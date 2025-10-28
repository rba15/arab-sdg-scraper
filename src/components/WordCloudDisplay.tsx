import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

interface WordCloudDisplayProps {
  words: Array<{
    word: string;
    count: number;
  }>;
}

export const WordCloudDisplay = ({ words }: WordCloudDisplayProps) => {
  const maxCount = Math.max(...words.map(w => w.count));
  
  const getFontSize = (count: number) => {
    const minSize = 14;
    const maxSize = 32;
    const size = minSize + ((count / maxCount) * (maxSize - minSize));
    return `${size}px`;
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Trending Keywords</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-3 justify-center items-center p-4">
          {words.map((word, index) => (
            <Badge
              key={index}
              variant="secondary"
              className="hover:bg-primary hover:text-primary-foreground transition-colors cursor-default"
              style={{ fontSize: getFontSize(word.count) }}
            >
              {word.word}
            </Badge>
          ))}
        </div>
      </CardContent>
    </Card>
  );
};
