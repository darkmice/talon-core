import { Component, type ReactNode, type ErrorInfo } from "react";
import i18n from "../i18n";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    void info;
    void error;
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex items-center justify-center h-screen bg-dark-800">
          <div className="text-center max-w-md px-6">
            <span className="material-symbols-outlined text-[48px] text-red-400 mb-4 block">error</span>
            <h1 className="text-lg font-bold text-white mb-2">{i18n.t("app.crashTitle")}</h1>
            <p className="text-sm text-slate-400 mb-4">{i18n.t("app.crashDesc")}</p>
            {this.state.error && (
              <pre className="bg-red-500/10 border border-red-500/30 text-red-400 text-xs font-mono rounded-lg px-4 py-3 text-left whitespace-pre-wrap break-all mb-4">
                {this.state.error.message}
              </pre>
            )}
            <button
              onClick={() => { this.setState({ hasError: false, error: null }); }}
              className="px-4 py-2 bg-primary hover:bg-primary-hover text-white text-sm font-medium rounded-lg transition"
            >
              {i18n.t("app.tryAgain")}
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
