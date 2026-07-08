export type ComposerImageAttachment = {
  readonly id: string;
  readonly fileName: string;
  readonly mimeType: string;
  readonly sizeBytes: number;
  readonly data: string;
  readonly previewUrl: string;
};

export type ComposerInputItem =
  | {
      readonly type: "text";
      readonly text: string;
    }
  | {
      readonly type: "image";
      readonly mediaType: string;
      readonly data: string;
    };

export type ComposerSendTurnPayload = {
  readonly sessionId: string;
  readonly text: string;
  readonly input: readonly ComposerInputItem[];
};

export const COMPOSER_IMAGE_MAX_COUNT = 4;
export const COMPOSER_IMAGE_MAX_BYTES = 8 * 1024 * 1024;

const ALLOWED_IMAGE_MIME_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif"
]);

let composerAttachmentCounter = 0;

export function createComposerAttachmentId(): string {
  composerAttachmentCounter += 1;
  return `composer_image_${Date.now().toString(36)}_${composerAttachmentCounter.toString(36)}`;
}

export function isAllowedComposerImage(file: File): boolean {
  return ALLOWED_IMAGE_MIME_TYPES.has(file.type);
}

export async function parseComposerImageAttachment(
  file: File
): Promise<ComposerImageAttachment> {
  if (!isAllowedComposerImage(file)) {
    throw new Error("Unsupported image type");
  }
  if (file.size > COMPOSER_IMAGE_MAX_BYTES) {
    throw new Error("Image is too large");
  }

  const dataUrl = await readFileAsDataUrl(file);
  const marker = ";base64,";
  const markerIndex = dataUrl.indexOf(marker);
  if (!dataUrl.startsWith("data:") || markerIndex === -1) {
    throw new Error("Could not read image data");
  }

  return {
    id: createComposerAttachmentId(),
    fileName: file.name || "image",
    mimeType: file.type,
    sizeBytes: file.size,
    data: dataUrl.slice(markerIndex + marker.length),
    previewUrl: dataUrl
  };
}

export function buildComposerSendTurnPayload(
  sessionId: string,
  text: string,
  attachments: readonly ComposerImageAttachment[]
): ComposerSendTurnPayload {
  const trimmedText = text.trim();
  const input: ComposerInputItem[] = [];
  if (trimmedText) {
    input.push({ type: "text", text: trimmedText });
  }
  for (const attachment of attachments) {
    input.push({
      type: "image",
      mediaType: attachment.mimeType,
      data: attachment.data
    });
  }
  return {
    sessionId,
    text: trimmedText,
    input
  };
}

export function formatComposerAttachmentSize(sizeBytes: number): string {
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }
  if (sizeBytes < 1024 * 1024) {
    return `${Math.round(sizeBytes / 102.4) / 10} KB`;
  }
  return `${Math.round(sizeBytes / (1024 * 102.4)) / 10} MB`;
}

function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error("Could not read image data"));
    reader.onload = () => {
      if (typeof reader.result === "string") {
        resolve(reader.result);
        return;
      }
      reject(new Error("Could not read image data"));
    };
    reader.readAsDataURL(file);
  });
}
