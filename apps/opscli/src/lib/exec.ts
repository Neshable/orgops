import { spawnSync, type SpawnSyncOptions } from "node:child_process";

export function npmCommandForHost() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

export function commandAvailable(command: string, args: string[] = ["--version"]) {
  const result = spawnSync(command, args, {
    stdio: "ignore",
    shell: process.platform === "win32",
    env: process.env,
  });
  return result.status === 0;
}

export function capture(
  command: string,
  args: string[],
  options?: { cwd?: string; env?: NodeJS.ProcessEnv }
) {
  return spawnSync(command, args, {
    cwd: options?.cwd ?? process.cwd(),
    encoding: "utf-8",
    shell: process.platform === "win32",
    env: options?.env ?? process.env,
  });
}

export function runChecked(
  command: string,
  args: string[],
  cwdOrOptions?: string | { cwd?: string; env?: NodeJS.ProcessEnv }
) {
  const cwd = typeof cwdOrOptions === "string" ? cwdOrOptions : cwdOrOptions?.cwd;
  const env = typeof cwdOrOptions === "string" ? process.env : cwdOrOptions?.env ?? process.env;
  const options: SpawnSyncOptions = {
    cwd: cwd ?? process.cwd(),
    stdio: "inherit",
    shell: process.platform === "win32",
    env,
  };
  const result = spawnSync(command, args, options);
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with status ${result.status}`);
  }
}
