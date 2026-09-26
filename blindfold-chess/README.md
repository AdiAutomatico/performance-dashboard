# Blindfold Chess Trainer (mockup)

Single-file prototype: `index.html`. No build step, no backend.

- Rules and move legality: chess.js (CDN)
- Opponent: Stockfish 10 (asm.js build, loaded from a CDN into a Web Worker), strength slider 1–8
- Voice in: browser Web Speech API (Chrome / Edge). Voice out: browser speech synthesis
- Blindfold: "Hide board" toggle, or say "hide board"
- Optional premium voice: ElevenLabs text-to-speech with your own API key (stored only in your browser)
- Piece images: the cburnett set (CC BY-SA 3.0, via chessground)

## Run it with the microphone

The claude.ai artifact preview blocks the microphone, so to talk to it open the file directly:

```
cd blindfold-chess
python3 -m http.server 8080
# then open http://localhost:8080 in Chrome and allow the microphone
```

Typing a command in the text box goes through the exact same parser as speech, so you can test phrasing without a mic.
