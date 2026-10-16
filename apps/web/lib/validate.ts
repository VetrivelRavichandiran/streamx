// Shared validation for topic creation (used by the Topics page).

export const TOPIC_NAME_RE = /^[a-zA-Z0-9._-]{1,64}$/;

export function validateTopicForm(name: string, partitions: number, rf: number): string | null {
  if (!TOPIC_NAME_RE.test(name)) return "Name: 1-64 chars, letters/digits/._- only";
  if (!Number.isInteger(partitions) || partitions < 1 || partitions > 64)
    return "Partitions must be an integer 1-64";
  if (!Number.isInteger(rf) || rf < 1 || rf > 3) return "Replication factor must be 1-3";
  return null;
}